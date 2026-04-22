#include "thread_pool.h"
#include "db_wrapper.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 * 함수 포인터 타입 정의.
 * 테스트 시 실제 queue_pop / queue_shutdown 대신 mock 함수를 주입할 수 있게
 * 전역 함수 포인터로 관리한다. (pool_set_queue_hooks_for_test 참고)
 */
typedef job_t *(*queue_pop_fn_t)(job_queue_t *queue);
typedef void (*queue_shutdown_fn_t)(job_queue_t *queue);

/* 전역 상태: 스레드 배열, 스레드 수, 큐 포인터, 함수 포인터 */
static pthread_t *g_workers = NULL;
static size_t g_worker_count = 0;
static job_queue_t *g_pool_queue = NULL;
static queue_pop_fn_t g_queue_pop_fn = queue_pop;
static queue_shutdown_fn_t g_queue_shutdown_fn = queue_shutdown;

/*
 * write_http_response — HTTP 200 응답을 client_fd에 쓴다.
 *
 * body는 JSON 문자열이며, Content-Length를 정확히 계산해서 헤더에 포함한다.
 * Connection: close를 명시해 클라이언트가 응답 끝을 알 수 있게 한다.
 */
static void write_http_response(int fd, const char *body)
{
    char header[256];
    size_t body_len = strlen(body);
    ssize_t n;

    n = snprintf(header, sizeof(header),
                 "HTTP/1.1 200 OK\r\n"
                 "Content-Type: application/json\r\n"
                 "Content-Length: %zu\r\n"
                 "Connection: close\r\n"
                 "\r\n",
                 body_len);

    write(fd, header, (size_t)n);
    write(fd, body, body_len);
}

/*
 * write_http_error — HTTP 에러 응답을 client_fd에 쓴다.
 *
 * DB 쿼리 실패 시 500 Internal Server Error 등을 반환할 때 사용한다.
 * body는 빈 JSON 객체 {} 로 고정한다.
 */
static void write_http_error(int fd, int code, const char *msg)
{
    char buf[256];
    ssize_t n;

    n = snprintf(buf, sizeof(buf),
                 "HTTP/1.1 %d %s\r\n"
                 "Content-Type: application/json\r\n"
                 "Content-Length: 2\r\n"
                 "Connection: close\r\n"
                 "\r\n"
                 "{}",
                 code, msg);

    write(fd, buf, (size_t)n);
}

/*
 * process_job — 큐에서 꺼낸 job 하나를 처리한다.
 *
 * 처리 순서:
 *   1. execute_query_safe()로 SQL을 실행하고 JSON 결과를 받는다.
 *   2. 성공하면 HTTP 200 응답, 실패하면 HTTP 500 응답을 client_fd에 쓴다.
 *   3. client_fd를 close한다.  (계약 C: worker가 close 책임)
 *   4. job->sql, job 자체를 free한다. (계약 A: worker가 free 책임)
 */
static void process_job(job_t *job)
{
    char *json = NULL;

    if (job == NULL) {
        return;
    }

    /* SQL 실행 → JSON 결과 획득 */
    if (execute_query_safe(job->sql, &json) == 0 && json != NULL) {
        write_http_response(job->client_fd, json);
        free(json); /* execute_query_safe가 malloc한 것을 여기서 해제 */
    } else {
        write_http_error(job->client_fd, 500, "Internal Server Error");
    }

    close(job->client_fd);  /* worker frees after process_job */
    free(job->sql);         /* worker frees after process_job */
    free(job);              /* worker frees after process_job */
}

/*
 * worker_loop — 각 worker 스레드가 실행하는 루프.
 *
 * queue_pop()은 job이 없으면 블로킹된다.
 * queue_shutdown() 후에는 NULL을 반환하므로, NULL이 오면 루프를 종료한다.
 * (계약 B: queue_pop이 NULL 반환 = shutdown 신호)
 */
static void *worker_loop(void *arg)
{
    job_queue_t *queue = (job_queue_t *)arg;
    /* g_queue_pop_fn을 지역 변수로 캡처: 테스트 중 hook이 바뀌어도 영향 없음 */
    queue_pop_fn_t pop_fn = g_queue_pop_fn;

    for (;;) {
        job_t *job = pop_fn(queue);

        if (job == NULL) {
            /* NULL = shutdown 신호. 루프 종료 후 스레드 종료 */
            break;
        }

        process_job(job);
    }

    return NULL;
}

/*
 * pool_init — num_workers개의 worker 스레드를 생성하고 풀을 초기화한다.
 *
 * 스레드 생성 중 실패하면, 이미 만든 스레드들을 shutdown/join으로 정리 후 -1 반환.
 * 반환값: 성공 0, 실패 -1
 */
int pool_init(size_t num_workers, job_queue_t *queue)
{
    pthread_t *workers;
    size_t created;

    if (num_workers == 0 || queue == NULL || g_workers != NULL) {
        return -1;
    }

    workers = (pthread_t *)calloc(num_workers, sizeof(*workers)); /* pool_destroy frees */
    if (workers == NULL) {
        return -1;
    }

    for (created = 0; created < num_workers; ++created) {
        if (pthread_create(&workers[created], NULL, worker_loop, queue) != 0) {
            size_t i;

            /* 생성 실패 시 이미 만들어진 스레드들을 정리 */
            g_queue_shutdown_fn(queue);
            for (i = 0; i < created; ++i) {
                pthread_join(workers[i], NULL);
            }
            free(workers);
            return -1;
        }
    }

    g_workers = workers;
    g_worker_count = num_workers;
    g_pool_queue = queue;

    return 0;
}

/*
 * pool_shutdown — 큐를 shutdown하고 모든 worker 스레드가 종료될 때까지 기다린다.
 *
 * queue_shutdown()이 브로드캐스트를 보내면 블로킹 중인 worker들이 깨어나
 * NULL을 받고 루프를 종료한다. pthread_join으로 전원 종료를 확인한다.
 */
void pool_shutdown(void)
{
    size_t i;

    if (g_workers == NULL || g_worker_count == 0) {
        return;
    }

    g_queue_shutdown_fn(g_pool_queue); /* worker들에게 종료 신호 전송 */

    for (i = 0; i < g_worker_count; ++i) {
        pthread_join(g_workers[i], NULL); /* 각 worker가 완전히 끝날 때까지 대기 */
    }

    g_worker_count = 0;
}

/*
 * pool_destroy — pool_shutdown 후 내부 리소스를 해제한다.
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
 * NULL을 넘기면 실제 queue_pop / queue_shutdown으로 복원된다.
 * 이 함수 덕분에 실제 큐 없이도 worker 동작을 단위 테스트할 수 있다.
 */
void pool_set_queue_hooks_for_test(job_t *(*pop_fn)(job_queue_t *),
                                   void (*shutdown_fn)(job_queue_t *))
{
    g_queue_pop_fn = (pop_fn != NULL) ? pop_fn : queue_pop;
    g_queue_shutdown_fn = (shutdown_fn != NULL) ? shutdown_fn : queue_shutdown;
}
