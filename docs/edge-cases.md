# 엣지 케이스 목록

각 파트가 방어해야 할 비정상 입력/상황 목록입니다.
Codex로 구현한 뒤 **반드시 이 목록을 보며 리뷰**하세요. AI가 놓치는 지점이 여기 많이 있습니다.

---

## Part 1 — 네트워크 서버 (석제)

### 바인딩 / 리슨
- [ ] 사용 중인 포트로 기동 시 명확한 에러 메시지 출력 후 종료
- [ ] `SO_REUSEADDR` 설정 (재시작 시 TIME_WAIT 회피)
- [ ] root 권한 없이 1024 미만 포트 요청 시 적절한 에러

### accept 루프
- [ ] `accept()`가 `EINTR`(시그널로 인한 중단) 반환 시 루프 계속 돌기
- [ ] `EMFILE`(fd 고갈) 시 서버 크래시 없이 로그만 남기고 계속
- [ ] `server_stop()` 호출 시 accept 루프가 깔끔히 종료 (블로킹 해제)

### 시그널
- [ ] `SIGPIPE` 무시 (클라이언트가 먼저 연결 끊어도 프로세스 안 죽음)
- [ ] `SIGINT` / `SIGTERM` 받으면 graceful shutdown 진입

---

## Part 2 — HTTP 프로토콜 (민철)

### 요청 읽기
- [ ] **부분 수신(partial read)** 처리 — `recv()`가 한 번에 전체를 못 읽을 수 있음, 루프로 읽기
- [ ] `Content-Length` 없으면 400
- [ ] `Content-Length`가 실제 body 크기와 다르면 처리 방식 결정 (timeout/400)
- [ ] 헤더만 오고 body가 안 와도 무한 대기하지 않도록 **read timeout 설정** (`SO_RCVTIMEO`)
- [ ] 요청 전체가 오기 전에 연결이 끊기면 fd 정리 후 종료

### 크기 제한
- [ ] **요청 body 최대 크기 제한** (예: 1MB). 초과 시 413 또는 400
- [ ] 헤더 라인 최대 길이 제한 (예: 8KB). 초과 시 431 또는 400
- [ ] SQL 문자열 최대 길이 제한

### 파싱
- [ ] 메서드가 POST/GET 아닌 경우 405
- [ ] 경로가 매칭되지 않으면 404
- [ ] `Content-Type`이 `application/json` 아니면 415 또는 400
- [ ] JSON 파싱 실패 시 400 (서버 크래시 X)
- [ ] `sql` 필드 없거나 문자열 아닐 때 400
- [ ] `sql` 필드가 빈 문자열("")일 때 400
- [ ] 멀티바이트 문자(한글, 이모지 등)가 SQL에 포함돼도 크래시 없이 전달

### 메모리
- [ ] sql 문자열 malloc 실패 시 500 응답 + fd close
- [ ] job_t malloc 실패 시 적절한 정리
- [ ] 에러 응답 후 반드시 `close(client_fd)` — fd 누수 방지

### 응답
- [ ] `send()`가 부분 전송할 수 있음 — 루프로 보내기
- [ ] `send()`가 `EPIPE` 반환 시 (클라이언트 먼저 끊음) 크래시 없이 처리

---

## Part 3 — 스레드 풀 (주형)

### 초기화
- [ ] `pthread_create` 실패 시 이미 생성된 스레드들을 정리하고 init 실패 반환
- [ ] worker 수 0 또는 음수 요청 시 에러
- [ ] worker 수가 과도하게 큰 경우 (예: 10000) 경고 또는 상한

### worker loop
- [ ] `queue_pop()`이 `NULL` 반환하면 즉시 루프 탈출 (shutdown 신호)
- [ ] `process_job()` 내부에서 예외 상황(파싱 실패, DB 에러) 발생 시 **fd와 메모리 반드시 정리**
- [ ] 한 job 처리가 오래 걸려도 다른 worker는 계속 동작

### process_job
- [ ] `execute_query` 실패 시 500 응답
- [ ] 응답 직렬화 실패 시 500 + 연결 종료
- [ ] 응답 전송 중 클라이언트 끊김 시 크래시 없음
- [ ] 처리 후 반드시 `close(job->client_fd)`, `free(job->sql)`, `free(job)` 순서

### 종료
- [ ] `pool_shutdown()`은 모든 worker를 `pthread_join`할 때까지 블로킹
- [ ] 종료 중에도 진행 중인 job은 끝까지 처리
- [ ] 큐에 남은 job 처리 정책 결정 (버리기 vs 끝까지 처리)
- [ ] 중복 shutdown 호출에 안전

---

## Part 4 — 큐 + 동기화 (지운)

### push
- [ ] 큐 가득 참 → 블로킹 (또는 reject, 정책에 따라)
- [ ] shutdown 중 push 요청 → -1 반환 (push 거부)
- [ ] NULL job 포인터 push 시 거부 (pop의 NULL 반환과 구분되어야 함)

### pop
- [ ] 큐 비어 있음 → 블로킹
- [ ] shutdown + 큐 비어 있음 → NULL 반환 (worker 종료 신호)
- [ ] shutdown + 큐에 job 남아 있음 → 남은 job 먼저 반환 (정책에 따라)

### 동기화
- [ ] **spurious wakeup** 방어 — `pthread_cond_wait`을 `while` 루프로 감싸기
- [ ] 조건 검사와 wait을 **같은 mutex 보호 하에서** 수행
- [ ] `not_empty` / `not_full` 두 condvar 모두 필요한 경우 사용
- [ ] `queue_shutdown`은 **두 condvar 모두 broadcast** (대기 중인 모든 push/pop 깨우기)
- [ ] shutdown 중 새로 블로킹 진입 시도 → 즉시 반환

### 자료구조
- [ ] **ring buffer**라면: 가득 참 판별 (`(tail+1) % capacity == head`) 정확성 확인
- [ ] **linked list**라면: 노드 malloc 실패 시 처리
- [ ] capacity 0 또는 음수 init 요청 거부

### 파괴
- [ ] `queue_destroy`는 모든 worker와 producer가 끝난 뒤에만 호출돼야 함 (AGENTS.md에 명시)
- [ ] destroy 내부에서 mutex / condvar 파괴 전 아무도 대기 중이지 않음을 보장

---

## 통합 단계 (전원)

### 생명주기
- [ ] main.c에서 `queue_init` → `pool_init` → `server_start` → `server_stop` → `queue_shutdown` → `pool_destroy` → `queue_destroy` 순서 확인
- [ ] SIGINT 핸들러가 `server_stop` 호출하도록 연결
- [ ] 핸들러 내에서는 `async-signal-safe` 함수만 사용 (write, _exit 등)

### 동시성 시나리오
- [ ] 동시 100 요청 — 응답 body가 다른 요청 body와 섞이지 않음
- [ ] 동시 INSERT — 데이터 정합성 (중복 없음, 누락 없음)
- [ ] 동시 INSERT + SELECT — SELECT가 부분 상태를 보지 않음 (Isolation)

### DB 엔진
- [ ] 동일 테이블에 동시 읽기 여러 개는 허용 (Reader-Writer Lock)
- [ ] 쓰기 중 읽기 대기 / 읽기 중 쓰기 대기 확인
- [ ] 기존 SQL 처리기가 전역 상태 갖고 있다면 그 접근 모두 lock 보호

### 리소스
- [ ] Valgrind `definitely lost: 0 bytes`
- [ ] helgrind 또는 ThreadSanitizer 레이스 보고 없음
- [ ] `lsof`로 fd 누수 없음 확인

---

## ⚠️ AI가 자주 놓치는 지점 (우선 체크)

1. **`recv` / `send`의 부분 수신/송신** — AI는 종종 한 번 호출로 충분하다고 가정함
2. **`pthread_cond_wait`의 while 루프** — spurious wakeup 방어 빠뜨림
3. **`queue_shutdown`에서 두 condvar broadcast** — 한쪽만 broadcast하면 반대쪽 대기자가 영원히 자고 있음
4. **`SIGPIPE` 무시 설정** — 안 하면 클라이언트가 중간에 끊으면 서버 프로세스 죽음
5. **`client_fd` 이중 close** — 에러 경로에서 close한 fd를 정상 경로에서 또 close
6. **`malloc` 실패 경로** — AI는 malloc 성공을 당연하게 가정
7. **worker 종료 시 남은 큐 처리** — 정책이 불명확하면 leak
