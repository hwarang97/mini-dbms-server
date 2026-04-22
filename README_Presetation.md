# 프로젝트 개요

SQL parser와 B+tree index로 구성된 dbengine을 api 서버를 통해 외부와 통신이 가능하게 만드는 프로젝트

---

## 핵심 구성

- server
    - 소켓을 생성하고, bind, listen, accept loop를 담당한다.
- http
    - HTTP 요청을 보고 파싱하여 SQL 구문을 다음으로 넘겨준다.
    - `POST /query`, `GET /health`, `GET /tables` 처리
- queue
    - http에서 보낸 job을 관리한다.
    - mutex와 condition variable을 기반으로 blocking 원형 큐를 사용한다.
- thread_pool
    - worker thread를 생성한다.
    - job_queue로부터 job을 받아서 job을 처리한다.
- db_wrapper
    - SQL parser / storage engine을 호출한다.
    - JSON 응답을 생성한다.
- main
    - queue, pool, server 초기화와 종료처리를 담당한다.


---

## 요청 처리 흐름

1. 클라이언트가 HTTP 요청을 보낸다.
2. 서버가 `accept()`로 연결을 받는다.
3. http.c가 요청을 파싱한다.
4. `POST /query`라면, SQL 문자열을 꺼내서 `job_t`를 생성합니다.
5. 생성된 job은 job_queue에 들어간다.
6. worker thread가 queue에서 job을 꺼낸다.
7. DB 엔진이 SQL을 실행하고 JSON 결과를 만든다.
8. worker가 HTTP 응답을 보내고, `client_fd`, `job->sql`, `job`을 정리한다.

```
┌─────────────────────────────────────────────────────────────┐
│                        클라이언트                            │
│         POST /query  {"sql": "SELECT * FROM users"}         │
└───────────────────────────┬─────────────────────────────────┘
                            │ TCP
┌───────────────────────────▼─────────────────────────────────┐
│  [Part 1 — 석제]  server.c                                  │
│  TCP 소켓 열기 → accept() → client_fd 생성                   │
│  handle_connection(client_fd) 호출                          │
└───────────────────────────┬─────────────────────────────────┘
                            │ client_fd
┌───────────────────────────▼─────────────────────────────────┐
│  [Part 2 — 주형]  http.c                                    │
│  HTTP 파싱 → SQL 추출 → job_t { client_fd, sql } 생성        │
│  queue_push(job)                                            │
└───────────────────────────┬─────────────────────────────────┘
                            │ job_t*
┌───────────────────────────▼─────────────────────────────────┐
│  [Part 4 — 지운]  job_queue.c                               │
│  스레드 안전한 큐 (mutex + condvar)                          │
│  비어있으면 worker 블로킹 / job 오면 즉시 깨움                │
└───────────────────────────┬─────────────────────────────────┘
                            │ queue_pop() → job_t*
┌───────────────────────────▼─────────────────────────────────┐
│  [Part 3 — 민철]  thread_pool.c + db_wrapper.c  ★           │
│                                                             │
│   worker #1 ──┐                                             │
│   worker #2 ──┤── execute_query_safe(sql)                   │
│   worker #3 ──┤── HTTP 응답 작성 → client_fd 전송            │
│   ...         │── close(fd) / free(sql, job)                │
│   worker #8 ──┘                                             │
└───────────────────────────┬─────────────────────────────────┘
                            │ HTTP 200 + JSON
┌───────────────────────────▼─────────────────────────────────┐
│                        클라이언트                            │
│         {"status":"ok","rows":[{"id":1,"name":"Alice"}]}    │
└─────────────────────────────────────────────────────────────┘
```

---

## 핵심 로직

### server - Atomic을 사용하는 이유
---

![atomic](./image/atomic.png)

atomic의 역할
- CPU 한 번짜리 연산 보장
- 컴파일러 재배치 제한
- 스레드 간 관찰 순서 규칙

이 세 가지를 같이 묶어서 보장해주는 장치


### thread_pool - thread_pool의 로직
---

![thread_pool & worker_loop](./image/thread_pool_work_loop_final.png)

Thread Pool은 미리 Worker Thread를 만들어두고, 각 Worker Thread가 Worker_loop()를 실행하도록 한다.

Worker_loop()는 큐에서 job을 꺼낸 뒤, process_job()에 job을 넘긴다.

만약 큐에 job이 없다면, 대기(블로킹)한다.

Process_job()은 SQL을 실행하고, HTTP 응답을 작성한 뒤, 소켓을 닫고, 메모리를 해제한다.

Pool_shutdown()은 큐에 종료 신호를 전송하고, 모든 Worker가 종료될 때까지 대기한다.



### queue - Producer-consumer 패턴과 채택 이유
---

![producer-consumer-pattern](./image/producer-consumer-circle.png)

producer-consumer 패턴이란
일을 시키는 스레드와 일을 수행하는 스레드로 나뉘어서 데이터를 주고 받는 동시성 패턴이다.

이 패턴을 채택한 이유는 요청을 받는 쪽과 SQL을 실행하는 쪽의 역할이 다르기 때문이다.

연결을 받는 쪽 = producer<br>
작업을 처리하는 worker = consumer
으로 나뉜다.

이 과제에서 특히 이 패턴이 더 잘 맞는 이유는 
1. 스레드 풀 요구사항과 딱 맞기 때문이다.
2. accept와 SQL 실행을 분리해야 서버가 안 막힌다.
3. 부하 조절이 쉬워진다

---

### 시연 명령어 모음

cmd 환경에서 아래의 명령어를 실행하여 시연하십시오.

```
서버 띄우기
docker exec -it ec454b06e960 sh -lc "cd /workspaces/mini-dbms-server && ./bin/dbms_server 8080"

동시 요청 10개 보내기
docker exec -it ec454b06e960 sh -lc "cd /workspaces/mini-dbms-server && bash ./scripts/run_concurrent_selects.sh"

e2e 테스트
docker exec -it ec454b06e960 sh -lc "cd /workspaces/mini-dbms-server && make e2e"
```

---

# 테스트 케이스

| 테스트 분류 | 테스트 이름 |  무엇을 테스트하는가 |
|---|---|---|
| 유닛 테스트 | `test_server` | `server_start()`, `server_stop()`와 accept loop가 정상적으로 동작하는지, 지정한 포트에 bind되는지, 연결을 `handle_connection`으로 넘기는지 확인합니다. |
| 유닛 테스트 | `test_http` | 현재는 테스트 바이너리가 정상 빌드되고 실행되는지만 확인하는 placeholder 성격의 테스트입니다. 본격적인 HTTP 파싱/에러 처리 케이스는 아직 상세하게 구현되어 있지 않습니다. |
| 유닛 테스트 | `test_pool` | thread pool 초기화 실패 조건, worker의 job 처리, 정상 SQL의 `200 OK` 응답, 잘못된 SQL의 `500 Internal Server Error` 응답, idle worker shutdown 동작을 확인합니다. |
| 유닛 테스트 | `test_queue` | blocking queue의 push/pop, shutdown wakeup, producer-consumer 동기화, 다중 producer/consumer 환경에서의 유실/중복 없는 처리, stress workload를 확인합니다. |
| E2E 테스트 | `make e2e` | 실제 `bin/dbms_server`를 띄운 뒤 `GET /health`, `GET /tables`, `POST /query` 기반 `CREATE`, `INSERT`, `SELECT`, `404`, `405`, 잘못된 SQL의 `500`까지 전체 요청 흐름을 검증합니다. 마지막에는 실제 저장된 schema/csv 내용도 화면에 출력합니다. |
| 리소스 테스트 | `check-leaks` | 실제 서버를 띄운 상태에서 요청을 보내고 Valgrind로 메모리 누수가 없는지 확인합니다. |
| 동시성 안정성 테스트 | `check-races` | 실제 서버를 띄운 상태에서 동시 요청을 보낸 뒤 Helgrind로 data race가 없는지 확인합니다. |
| 리소스 안정성 테스트 | `check-fd` | 실제 서버에 반복 요청을 보내면서 파일 디스크립터 수가 비정상적으로 증가하지 않는지 확인합니다. |
| 동시 요청 확인 | `concurrent_selects` | 서버가 이미 실행 중일 때 `SELECT * FROM users` 요청 10개를 동시에 보내고, 각 요청이 응답 코드를 돌려주는지 확인합니다. |