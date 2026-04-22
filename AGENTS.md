# AGENTS.md — 미니 DBMS API 서버

이 문서는 Codex CLI가 프로젝트 작업 시 따라야 할 규칙입니다.
4명의 팀원이 각자 로컬에서 Codex CLI를 돌리며, 각 파트별 브랜치에서 작업합니다.

---

## 프로젝트 개요

**목표:** C 언어로 미니 DBMS API 서버를 구현한다.

- TCP 소켓 기반 HTTP 서버
- Thread Pool로 SQL 요청을 병렬 처리
- 기존 SQL 처리기 & B+ 트리 인덱스를 내부 DB 엔진으로 활용
- 멀티스레드 환경에서 안전하게 동작해야 함

**빌드:** `make`
**테스트:** `make test`
**실행:** `./bin/dbms_server 8080`

---

## 절대 규칙

1. **테스트가 통과하지 못하면 커밋하지 않는다.** 커밋 전 반드시 `make test`를 실행하고 전부 통과해야 한다.
2. **공유 헤더(common.h, job_queue.h 등 계약에 해당하는 파일)는 절대 임의로 수정하지 않는다.** 수정이 필요하면 팀 전체에 먼저 공지한다.
3. **자신의 파트 디렉토리 외의 파일은 수정하지 않는다.** 통합 단계에서만 팀 전체가 함께 건드린다.
4. **플랫폼 의존 코드를 작성하지 않는다.** macOS / Linux (WSL) 양쪽에서 `pthread` 기반으로 동작해야 한다.
5. **`malloc`한 메모리는 책임 주체를 주석에 명시한다.** (예: `// caller frees`, `// worker frees after process_job`)

---

## 디렉토리 구조

```
.
├── AGENTS.md              ← 이 파일
├── CLAUDE.md              ← Claude용 동일 규칙
├── COMMIT_RULES.md        ← 커밋 규칙
├── Makefile
├── include/
│   ├── common.h           ← job_t, 전역 큐 선언 (공유)
│   ├── job_queue.h        ← queue API (공유)
│   ├── thread_pool.h      ← pool API (공유)
│   ├── server.h           ← server API (공유)
│   └── http.h             ← handle_connection (공유)
├── src/
│   ├── main.c
│   ├── server/            ← 석제 (Part 1)
│   ├── http/              ← 민철 (Part 2)
│   ├── pool/              ← 주형 (Part 3)
│   └── queue/             ← 지운 (Part 4)
├── tests/
│   ├── test_server.c
│   ├── test_http.c
│   ├── test_pool.c
│   └── test_queue.c
└── external/              ← 기존 SQL 처리기 + B+ 트리
```

---

## 계약(Contract) — 절대 변경 금지

### 계약 A — `job_t` (common.h)

```c
typedef struct {
    int   client_fd;   // 응답 쓸 소켓. worker가 close 책임짐
    char *sql;         // malloc된 SQL 문자열. worker가 free 책임짐
} job_t;
```

### 계약 B — Queue API (job_queue.h)

```c
typedef struct job_queue job_queue_t;

job_queue_t *queue_init(size_t capacity);
int          queue_push(job_queue_t *q, job_t *job);  // 큐 풀이면 block
job_t       *queue_pop(job_queue_t *q);               // 비면 block, shutdown 시 NULL
void         queue_shutdown(job_queue_t *q);
void         queue_destroy(job_queue_t *q);
```

`queue_pop`이 NULL을 반환하면 "shutdown 신호, worker 종료" 규약.

### 계약 C — `client_fd` 소유권

| 상황 | 누가 close |
|---|---|
| accept 성공 → `handle_connection` 호출 | 민철이 소유 |
| 민철 HTTP 파싱 실패 (400) | 민철이 에러 응답 후 close |
| 민철 파싱 성공 → queue_push | worker(주형)가 소유 |
| worker SQL 실행 + 응답 완료 | worker가 close |

### 계약 D — 엔트리 포인트 (http.h)

```c
void handle_connection(int client_fd);   // 민철이 구현, 석제가 호출
```

---

## Codex CLI 작업 프롬프트 (각 파트별)

각 팀원은 자신의 브랜치로 체크아웃 후, 아래 프롬프트를 상황에 맞게 Codex에 입력한다.

---

### Part 1 (석제) — 네트워크 서버 뼈대

**최초 프롬프트:**

```
This repository is a C-based mini DBMS API server. Read AGENTS.md first.

I'm responsible for Part 1: the TCP socket server skeleton.
My files are under src/server/ and I expose the API defined in include/server.h.

Implement src/server/server.c with:
- server_start(int port): creates socket, binds, listens, runs an accept loop
- server_stop(void): gracefully stops the accept loop
- On each accepted connection, call handle_connection(client_fd) (declared in http.h).
  Do NOT parse HTTP here. That is Part 2's job.
- Use SO_REUSEADDR. Handle EINTR on accept.
- Do not read from or close client_fd here. handle_connection owns it.

Write unit tests in tests/test_server.c using a mock handle_connection
that writes the fd to a shared list. Verify:
- server binds to the given port
- accept loop delivers fds to handle_connection
- server_stop breaks the loop cleanly

Run `make test` after. Must pass before committing.
Do not modify any file outside src/server/ and tests/test_server.c.
```

---

### Part 2 (민철) — HTTP 프로토콜 처리

**최초 프롬프트:**

```
This repository is a C-based mini DBMS API server. Read AGENTS.md first.

I'm responsible for Part 2: HTTP protocol handling.
My files are under src/http/ and I expose handle_connection(int) from http.h.

Implement src/http/http.c:
- Read HTTP request from client_fd (handle partial reads, Content-Length)
- Parse method, path, body
- For POST /query with JSON body {"sql": "..."}:
    - extract the sql string (malloc a copy)
    - build a job_t with {client_fd, sql} and call queue_push(g_queue, job)
    - DO NOT close client_fd on success — the worker will
- For any parse error or unsupported route:
    - write a 400/404/500 response
    - close client_fd
    - free any allocations

Memory rule: sql string is malloc'd here, freed by the worker after process_job.

Write tests in tests/test_http.c with a mock queue that captures pushed jobs.
Verify: valid request produces correct job, malformed request produces error response,
oversized body is rejected.

Run `make test`. Do not modify files outside src/http/ and tests/test_http.c.
```

---

### Part 3 (주형) — 스레드 풀

**최초 프롬프트:**

```
This repository is a C-based mini DBMS API server. Read AGENTS.md first.

I'm responsible for Part 3: the thread pool.
My files are under src/pool/ and I expose the API in include/thread_pool.h.

Implement src/pool/thread_pool.c:
- pool_init(num_workers, queue): spawn num_workers pthreads
- Each worker loops: job = queue_pop(q); if (job == NULL) break; process_job(job);
- process_job(job_t *) should:
    - call execute_query(job->sql, &result) from the DB engine
    - format result as JSON
    - write HTTP response to job->client_fd
    - close(job->client_fd)
    - free(job->sql); free(job)
- pool_shutdown: call queue_shutdown, then pthread_join all workers
- pool_destroy: free internal resources

For now, stub execute_query to return a fixed result. The DB engine integration
comes later during integration phase.

Write tests in tests/test_pool.c:
- push N jobs with a mock process_job that increments a counte
- verify all jobs processed
- verify shutdown joins all threads cleanly

Run `make test`. Do not modify files outside src/pool/ and tests/test_pool.c.
```

---

### Part 4 (지운) — 큐 + 동기화

**최초 프롬프트:**

```
This repository is a C-based mini DBMS API server. Read AGENTS.md first.

I'm responsible for Part 4: the thread-safe job queue.
My files are under src/queue/ and I expose the API in include/job_queue.h.

Implement src/queue/job_queue.c:
- Internally, use a fixed-size ring buffer OR linked list, protected by
  pthread_mutex_t + two pthread_cond_t (not_empty, not_full)
- queue_push blocks when full. Returns -1 if queue is shut down.
- queue_pop blocks when empty. Returns NULL if queue is shut down and empty.
- queue_shutdown sets a flag and broadcasts both condvars so every waiter wakes up.
- queue_destroy frees resources (caller must ensure no more push/pop).

Write tests in tests/test_queue.c:
- single producer + single consumer: N items in, N items out
- multiple producers + multiple consumers: no lost or duplicated jobs
- queue_shutdown wakes all blocked poppers with NULL return
- stress test: 4 producers + 4 consumers, 10000 items, verify count

Run `make test`. Use helgrind or ThreadSanitizer if available.
Do not modify files outside src/queue/ and tests/test_queue.c.
```

---

## 통합 단계 프롬프트 (15:00 이후, 전원 공유)

통합 시점에 누구든 사용 가능:

```
All four parts now have passing unit tests. Switch from mock implementations
to real ones and wire them together in src/main.c:

1. queue_init(128)
2. pool_init(8, queue)
3. server_start(8080) — blocks on accept loop
4. On SIGINT: server_stop, queue_shutdown, pool_destroy, queue_destroy

Run the full server with `./bin/dbms_server 8080` and send requests with:
    curl -X POST http://localhost:8080/query \
         -H "Content-Type: application/json" \
         -d '{"sql": "SELECT * FROM users"}'

Verify:
- single request works
- 100 concurrent requests work (use `ab` or a shell loop)
- no leaks under Valgrind
- no races under helgrind / ThreadSanitize
```

---

## Codex CLI 사용 팁

- **항상 `AGENTS.md`를 먼저 읽게 한다.** 프롬프트 첫 줄에 `Read AGENTS.md first`를 넣는다.
- **범위를 좁게 지정한다.** "Part 2 작업. src/http/ 외 파일 수정 금지"를 명시.
- **테스트 작성을 명시적으로 요청한다.** 요청하지 않으면 빠뜨린다.
- **커밋은 Codex에 맡기지 말고 직접 한다.** 테스트 통과 확인 후 수동 커밋.
- **의심되는 코드는 "왜 이렇게 짰는지" 한 줄씩 설명 요청한다.** 발표 때 설명할 수 있어야 한다.

---

## 금지 사항

- Windows 전용 API 사용 (WinSock 등)
- 글로벌 mutex 하나로 모든 걸 보호하는 식의 게으른 동기화
- `sleep()`으로 동시성 테스트 회피
- 테스트 없이 커밋
- 다른 파트 디렉토리 수정
