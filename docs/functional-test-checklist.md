# 기능 테스트 체크리스트

단위 테스트(`make test`) 통과 후, 서버를 실제로 띄워놓고 수행하는 API 레벨 테스트입니다.
통합 단계(수요일 15:00 이후)에 전원이 함께 돌려봅니다.

**준비**
```bash
./bin/dbms_server 8080 &
SERVER_PID=$!
```

---

## A. 정상 동작 (Happy Path)

- [ ] **A1. 서버 헬스체크**
  ```bash
  curl -i http://localhost:8080/health
  ```
  기대: `200 OK`

- [ ] **A2. 단순 SELECT**
  ```bash
  curl -X POST http://localhost:8080/query \
    -H "Content-Type: application/json" \
    -d '{"sql": "SELECT * FROM users"}'
  ```
  기대: `200 OK`, JSON body에 `rows` / `count`

- [ ] **A3. INSERT 후 SELECT**
  ```bash
  curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
    -d '{"sql": "INSERT INTO users VALUES (1, \"alice\")"}'
  curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
    -d '{"sql": "SELECT * FROM users WHERE id = 1"}'
  ```
  기대: 삽입한 데이터가 조회됨

- [ ] **A4. 테이블 목록 조회**
  ```bash
  curl http://localhost:8080/tables
  ```
  기대: `200 OK`, 테이블 이름 배열

---

## B. 에러 응답 (Error Path)

- [ ] **B1. 빈 SQL**
  ```bash
  curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
    -d '{"sql": ""}'
  ```
  기대: `400 Bad Request`

- [ ] **B2. 잘못된 JSON**
  ```bash
  curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
    -d '{broken'
  ```
  기대: `400 Bad Request`, 서버 크래시 없음

- [ ] **B3. sql 필드 누락**
  ```bash
  curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
    -d '{"other": "value"}'
  ```
  기대: `400 Bad Request`

- [ ] **B4. 잘못된 SQL 문법**
  ```bash
  curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
    -d '{"sql": "SELEKT * FRUM users"}'
  ```
  기대: `400` 또는 `500`, 에러 메시지 포함

- [ ] **B5. 존재하지 않는 테이블**
  ```bash
  curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
    -d '{"sql": "SELECT * FROM no_such_table"}'
  ```
  기대: 적절한 에러 코드 + 메시지

- [ ] **B6. 존재하지 않는 엔드포인트**
  ```bash
  curl -i http://localhost:8080/fooba
  ```
  기대: `404 Not Found`

- [ ] **B7. 허용되지 않는 HTTP 메서드**
  ```bash
  curl -i -X DELETE http://localhost:8080/query
  ```
  기대: `405 Method Not Allowed` 또는 `404`

---

## C. 동시성 / 부하

- [ ] **C1. 동시 SELECT 100건**
  ```bash
  for i in {1..100}; do
    curl -s -X POST http://localhost:8080/query \
      -H "Content-Type: application/json" \
      -d '{"sql": "SELECT * FROM users"}' &
  done
  wait
  ```
  기대: 모두 200, 응답 꼬임 없음, 서버 살아있음

- [ ] **C2. 동시 INSERT + SELECT 혼합**
  ```bash
  for i in {1..50}; do
    curl -s -X POST http://localhost:8080/query -H "Content-Type: application/json" \
      -d "{\"sql\": \"INSERT INTO users VALUES ($i, 'u$i')\"}" &
    curl -s -X POST http://localhost:8080/query -H "Content-Type: application/json" \
      -d '{"sql": "SELECT * FROM users"}' &
  done
  wait
  ```
  기대: 데이터 정합성 유지 (중복 없음, 누락 없음)

- [ ] **C3. Apache Bench (있다면)**
  ```bash
  ab -n 1000 -c 20 -p payload.json -T application/json \
    http://localhost:8080/query
  ```
  기대: 실패 요청 0, 평균 응답 시간 합리적

- [ ] **C4. 장시간 부하 (30초)**
  ```bash
  timeout 30 bash -c 'while true; do
    curl -s -X POST http://localhost:8080/query \
      -H "Content-Type: application/json" \
      -d "{\"sql\": \"SELECT * FROM users\"}" > /dev/null
  done'
  ```
  기대: 메모리 누수 없음 (`ps` 또는 Valgrind)

---

## D. 종료 / 생명주기

- [ ] **D1. SIGINT 종료**
  ```bash
  kill -INT $SERVER_PID
  ```
  기대: worker 전부 join, 포트 정상 해제, 좀비 프로세스 없음

- [ ] **D2. 재시작 가능 여부**
  ```bash
  ./bin/dbms_server 8080
  # → Ctrl+C
  ./bin/dbms_server 8080   # 바로 다시 실행
  ```
  기대: `Address already in use` 없이 정상 기동 (SO_REUSEADDR)

- [ ] **D3. 처리 중 종료 시 진행 요청 처리**
  <!-- 선택 구현: graceful shutdown이 진행 중인 요청을 끝까지 처리하는지 -->

---

## E. 메모리 / 리소스

- [ ] **E1. Valgrind 검사**
  ```bash
  valgrind --leak-check=full --error-exitcode=1 ./bin/dbms_server 8080
  # 다른 터미널에서 몇 개 요청 후 SIGINT
  ```
  기대: `definitely lost: 0 bytes`

- [ ] **E2. helgrind 또는 ThreadSanitizer**
  ```bash
  valgrind --tool=helgrind ./bin/dbms_server 8080
  # 또는 -fsanitize=thread로 빌드
  ```
  기대: data race 보고 없음

- [ ] **E3. fd 누수 체크**
  ```bash
  lsof -p $SERVER_PID | wc -l   # 요청 전
  # 100건 요청 후
  lsof -p $SERVER_PID | wc -l   # 요청 후
  ```
  기대: 숫자가 계속 증가하지 않음

---

## ✅ 발표 데모용 추천 시나리오 (4분)

1. **A1 헬스체크** (10초) — 서버 띄워져 있음 보여주기
2. **A3 INSERT + SELECT** (30초) — 기본 동작
3. **B2 잘못된 JSON** (20초) — 서버 안 죽음
4. **C2 동시 INSERT+SELECT** (60초) — 진짜 볼거리. 터미널 여러 개 띄워서 병렬 실행
5. **트러블슈팅 1~2개 공유** (90초) — "이런 버그가 있었고 이렇게 잡았습니다"
6. **Valgrind 결과** (20초) — 스크린샷으로 마무리
