/*
 * thread_pool.c — 스레드 풀 구현
 *
 * 이 파일이 하는 일:
 *   HTTP 요청으로 들어온 SQL job을 여러 worker 스레드가 동시에 처리할 수 있도록
 *   스레드 풀을 만들고 관리한다.
 *
 * 전체 흐름:
 *   1. pool_init()     → worker 스레드 N개 생성. 각 스레드는 worker_loop() 실행
 *   2. worker_loop()   → 큐에서 job을 꺼내서 process_job() 에 넘김. job이 없으면 대기.
 *   3. process_job()   → SQL 실행 → HTTP 응답 작성 → 소켓 닫기 → 메모리 해제
 *   4. pool_shutdown() → 큐에 종료 신호 전송 → 모든 worker가 종료될 때까지 대기
 *   5. pool_destroy()  → 스레드 배열 메모리 해제
 */

#include "thread_pool.h"
#include "db_wrapper.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 * 함수 포인터 타입 정의.
 *
 * 왜 함수 포인터를 쓰는가?
 *   실제 queue_pop / queue_shutdown 은 블로킹 동작이라 단위 테스트가 어렵다.
 *   (블로킹 = job이 없으면 스레드가 거기서 멈춰서 기다림.
 *    테스트 코드가 job을 안 넣으면 worker가 영원히 기다려서 테스트가 끝나지 않음.)
 *   함수 포인터로 관리하면 테스트 시 mock 함수로 교체할 수 있다.
 *   실제 서버 실행 시에는 기본값(queue_pop, queue_shutdown)이 그대로 사용된다.
 */
typedef job_t *(*queue_pop_fn_t)(job_queue_t *queue);
typedef void (*queue_shutdown_fn_t)(job_queue_t *queue);
typedef int (*query_execute_fn_t)(const char *sql, char **out_json);

/*
 * 전역 상태 변수.
 *
 * g_workers      : pthread_create로 만든 worker 스레드 ID 배열
 * g_worker_count : 생성된 worker 스레드 수
 * g_pool_queue   : worker들이 job을 꺼낼 큐
 * g_queue_pop_fn / g_queue_shutdown_fn : 실제 함수 or 테스트용 mock 함수
 */
static pthread_t           *g_workers          = NULL;
static size_t               g_worker_count     = 0;
static job_queue_t         *g_pool_queue       = NULL;
static queue_pop_fn_t       g_queue_pop_fn     = queue_pop;
static queue_shutdown_fn_t  g_queue_shutdown_fn = queue_shutdown;
static query_execute_fn_t   g_query_execute_fn = execute_query_safe;

/*
 * write_http_response — HTTP 200 OK 응답을 소켓(fd)에 쓴다.
 *
 * HTTP 응답은 두 부분으로 구성된다:
 *   1. 헤더: 상태 코드, Content-Type, Content-Length 등
 *   2. 빈 줄(\r\n\r\n): 헤더와 본문의 구분자
 *   3. 본문: JSON 문자열
 *
 * Content-Length를 정확히 계산해야 클라이언트가 응답이 언제 끝나는지 알 수 있다.
 * Connection: close 를 명시해 응답 후 연결을 끊겠다는 것을 알린다.
 *
 * 실제 클라이언트가 받는 모습:
 *   HTTP/1.1 200 OK
 *   Content-Type: application/json
 *   Content-Length: 35
 *   Connection: close
 *
 *   {"status":"ok","query":"SELECT 1"}
 */
static void write_http_response(int fd, const char *body)
{
    char header[256];
    size_t body_len = strlen(body);
    ssize_t n;

    /* 헤더 문자열 생성. snprintf는 실제로 쓴 바이트 수를 반환한다. */
    n = snprintf(header, sizeof(header),
                 "HTTP/1.1 200 OK\r\n"
                 "Content-Type: application/json\r\n"
                 "Content-Length: %zu\r\n"
                 "Connection: close\r\n"
                 "\r\n",             /* 헤더와 본문 사이 빈 줄 */
                 body_len);

    write(fd, header, (size_t)n);   /* 헤더 전송 */
    write(fd, body, body_len);      /* 본문(JSON) 전송 */
}

/*
 * write_http_error — HTTP 에러 응답을 소켓(fd)에 쓴다.
 *
 * SQL 실행 실패 등 서버 내부 오류 시 사용한다.
 * 본문은 빈 JSON 객체 {} 로 고정한다.
 *
 * 예: code=500, msg="Internal Server Error" 이면
 *   HTTP/1.1 500 Internal Server Error
 *   ...
 *   {}
 */
static void write_http_error(int fd, int code, const char *msg)
{
    char buf[256];
    ssize_t n;

    /* 헤더 + 본문을 하나의 버퍼에 담아 한 번에 전송 */
    n = snprintf(buf, sizeof(buf),
                 "HTTP/1.1 %d %s\r\n"
                 "Content-Type: application/json\r\n"
                 "Content-Length: 2\r\n"    /* {} 는 2바이트 */
                 "Connection: close\r\n"
                 "\r\n"
                 "{}",
                 code, msg);

    write(fd, buf, (size_t)n);
}

/*
 * process_job — 큐에서 꺼낸 job 하나를 처리한다.
 *
 * job 안에는 두 가지 정보가 있다:
 *   job->sql       : 실행할 SQL 문자열 (예: "SELECT * FROM users")
 *   job->client_fd : 결과를 보낼 클라이언트 소켓
 *
 * 처리 순서:
 *   1. execute_query_safe(sql) 호출 → SQL 실행 → JSON 결과 문자열 획득
 *   2. 성공 → write_http_response() 로 HTTP 200 + JSON 전송
 *      실패 → write_http_error() 로 HTTP 500 전송
 *   3. client_fd 닫기  (계약 C: 파싱 성공 후 worker가 close 책임)
 *   4. job->sql 해제   (계약 A: worker가 free 책임)
 *   5. job 자체 해제   (계약 A: worker가 free 책임)
 *
 * 주의: job->sql 과 job 은 Part 2(민철)가 malloc 했지만,
 *       free 는 여기서 한다. (소유권이 worker에게 넘어온 것)
 */
static void process_job(job_t *job)
{
    char *json = NULL;

    if (job == NULL) {
        return;
    }

    /* SQL을 실행하고 결과를 JSON 문자열로 받는다.
     * 성공 시 json에 malloc된 문자열이 들어온다. */
    if (g_query_execute_fn(job->sql, &json) == 0 && json != NULL) {
        write_http_response(job->client_fd, json);
        free(json); /* execute_query_safe가 malloc한 것을 여기서 해제 */
    } else {
        /* SQL 실행 실패 → 클라이언트에 500 에러 응답 */
        write_http_error(job->client_fd, 500, "Internal Server Error");
    }

    close(job->client_fd);  /* worker frees after process_job (계약 C) */
    free(job->sql);         /* worker frees after process_job (계약 A) */
    free(job);              /* worker frees after process_job (계약 A) */
}

/*
 * worker_loop — 각 worker 스레드가 실행하는 무한 루프.
 *
 * 동작 방식:
 *   - queue_pop()을 호출해 job을 가져온다.
 *   - job이 없으면 queue_pop() 내부에서 블로킹(대기) 상태가 된다.
 *     → CPU를 낭비하지 않고 조용히 기다림 (sleep과 달리 즉시 깨어날 수 있음)
 *   - job이 들어오면 자동으로 깨어나서 process_job() 실행
 *   - queue_shutdown() 이 호출되면 queue_pop()이 NULL을 반환
 *     → NULL을 받으면 루프를 빠져나가 스레드 종료
 *
 * arg: pool_init에서 넘겨준 job_queue_t 포인터
 */
static void *worker_loop(void *arg)
{
    job_queue_t *queue = (job_queue_t *)arg;

    /*
     * g_queue_pop_fn을 지역 변수로 캡처한다.
     * 스레드 시작 시점의 함수 포인터를 고정해두는 것.
     * 테스트 도중 전역 hook이 바뀌어도 이 스레드는 영향을 받지 않는다.
     */
    queue_pop_fn_t pop_fn = g_queue_pop_fn;

    for (;;) {
        /* job이 없으면 여기서 블로킹. job이 오면 반환. shutdown이면 NULL 반환. */
        job_t *job = pop_fn(queue);

        if (job == NULL) {
            /* NULL = shutdown 신호. 루프 탈출 후 스레드 종료 */
            break;
        }

        process_job(job);
    }

    return NULL;
}

/*
 * pool_init — num_workers개의 worker 스레드를 생성하고 풀을 초기화한다.
 *
 * 파라미터:
 *   num_workers : 생성할 worker 스레드 수 (예: 8)
 *   queue       : worker들이 job을 꺼낼 큐
 *
 * 동작:
 *   1. 스레드 ID를 저장할 배열(pthread_t[]) 을 calloc으로 할당
 *   2. pthread_create로 스레드를 하나씩 생성. 각 스레드는 worker_loop 실행
 *   3. 생성 도중 실패하면 이미 만든 스레드들을 shutdown/join으로 정리 후 -1 반환
 *
 * 반환값: 성공 0, 실패 -1
 */
int pool_init(size_t num_workers, job_queue_t *queue)
{
    pthread_t *workers;
    size_t created;

    /* 이미 초기화된 상태거나 잘못된 인자면 거부 */
    if (num_workers == 0 || queue == NULL || g_workers != NULL) {
        return -1;
    }

    /* worker 스레드 ID 배열 할당. pool_destroy에서 해제. */
    workers = (pthread_t *)calloc(num_workers, sizeof(*workers)); /* pool_destroy frees */
    if (workers == NULL) {
        return -1;
    }

    /* 스레드를 하나씩 생성 */
    for (created = 0; created < num_workers; ++created) {
        if (pthread_create(&workers[created], NULL, worker_loop, queue) != 0) {
            size_t i;

            /*
             * 스레드 생성 실패.
             * 이미 만든 스레드(0 ~ created-1)들은 shutdown 신호를 보내고
             * pthread_join으로 종료될 때까지 기다린 뒤 배열을 해제한다.
             */
            g_queue_shutdown_fn(queue);
            for (i = 0; i < created; ++i) {
                pthread_join(workers[i], NULL);
            }
            free(workers);
            return -1;
        }
    }

    /* 전역 상태에 저장 */
    g_workers = workers;
    g_worker_count = num_workers;
    g_pool_queue = queue;

    return 0;
}

/*
 * pool_shutdown — 모든 worker 스레드를 안전하게 종료시킨다.
 *
 * 동작:
 *   1. queue_shutdown() 으로 큐에 종료 신호 전송
 *      → 블로킹 중인 worker들이 NULL을 받고 루프를 탈출
 *   2. pthread_join() 으로 각 worker가 완전히 끝날 때까지 대기
 *      → 이게 없으면 worker가 아직 실행 중인데 메모리를 해제할 수 있음
 */
void pool_shutdown(void)
{
    size_t i;

    if (g_workers == NULL || g_worker_count == 0) {
        return;
    }

    /* 큐에 종료 신호 보내기 → worker들이 NULL 받고 루프 탈출 */
    g_queue_shutdown_fn(g_pool_queue);

    /* 모든 worker 스레드가 완전히 종료될 때까지 기다리기 */
    for (i = 0; i < g_worker_count; ++i) {
        pthread_join(g_workers[i], NULL);
    }

    g_worker_count = 0;
}

/*
 * pool_destroy — pool_shutdown 후 내부 리소스를 해제한다.
 *
 * pool_shutdown을 내부에서 호출하므로 별도로 shutdown을 부를 필요 없다.
 * pool_init에서 calloc한 스레드 배열을 여기서 해제한다.
 */
void pool_destroy(void)
{
    pool_shutdown();

    free(g_workers); /* pool_init에서 calloc한 스레드 배열 해제 */
    g_workers = NULL;
    g_pool_queue = NULL;
}

/*
 * pool_set_queue_hooks_for_test — 테스트용 mock 함수를 주입한다.
 *
 * 실제 queue_pop / queue_shutdown 은 블로킹 동작이라 단위 테스트하기 어렵다.
 * 이 함수로 mock을 주입하면 실제 큐 없이 worker 동작만 따로 테스트할 수 있다.
 *
 * NULL을 넘기면 실제 함수로 복원된다.
 * 테스트가 끝나면 반드시 NULL로 복원해야 한다.
 */
void pool_set_queue_hooks_for_test(job_t *(*pop_fn)(job_queue_t *),
                                   void (*shutdown_fn)(job_queue_t *))
{
    g_queue_pop_fn     = (pop_fn      != NULL) ? pop_fn      : queue_pop;
    g_queue_shutdown_fn = (shutdown_fn != NULL) ? shutdown_fn : queue_shutdown;
}

void pool_set_query_executor_for_test(int (*executor_fn)(const char *sql, char **out_json))
{
    g_query_execute_fn = (executor_fn != NULL) ? executor_fn : execute_query_safe;
}
