# Part 0 — 저장소 초기 세팅 (한 명이 수행)

한 명(예: 석제)이 수요일 10:00~10:30 사이에 **혼자** 저장소 골격을 만들고 `main` 브랜치에 푸시합니다.
이 세팅이 끝나야 나머지 3명이 브랜치를 파고 작업을 시작할 수 있습니다.

---

## 1. 사전 준비 (세팅 담당자가 수동으로)

### 저장소 생성

```bash
# GitHub에서 빈 레포 "mini-dbms-server" 생성 (README/gitignore 없이)
git clone git@github.com:<ORG>/mini-dbms-server.git
cd mini-dbms-serve
```

### 4개 문서 배치

이미 만들어둔 4개 파일을 레포 루트에 복사:

```bash
cp ~/Downloads/AGENTS.md            .
cp ~/Downloads/CLAUDE.md            .        # 없으면 AGENTS.md 복사본
cp ~/Downloads/COMMIT_RULES.md      .
cp ~/Downloads/README.md            .
mkdir -p docs
cp ~/Downloads/functional-test-checklist.md  docs/
cp ~/Downloads/edge-cases.md                 docs/
```

### cJSON 드롭인

```bash
mkdir -p external/cjson
curl -sSL -o external/cjson/cJSON.h https://raw.githubusercontent.com/DaveGamble/cJSON/master/cJSON.h
curl -sSL -o external/cjson/cJSON.c https://raw.githubusercontent.com/DaveGamble/cJSON/master/cJSON.c
```

### 기존 DB 엔진 배치

```bash
mkdir -p external/db_engine
# 이전 차수에서 만든 SQL 처리기 + B+ 트리 소스를 여기에 복사
cp -r ~/정글/prev_week/sql_processor/*.{c,h}   external/db_engine/
cp -r ~/정글/prev_week/bplustree/*.{c,h}       external/db_engine/
```

---

## 2. Codex CLI 프롬프트 (세팅 담당자가 실행)

아래 프롬프트를 Codex CLI에 그대로 넣습니다.

```
I am setting up a new C project: a mini DBMS API server.
Read AGENTS.md, CLAUDE.md, and COMMIT_RULES.md first to understand the rules.

I am "Part 0" — the one-time initial setup. After this, 4 teammates will
branch off main and implement Parts 1–4 in parallel. My job is to produce
a skeleton that compiles, has empty stubs for every contract, and passes
`make test` with placeholder tests.

=== Task 1: Directory skeleton ===

Create these directories if missing:
- include/
- src/server/
- src/http/
- src/pool/
- src/queue/
- tests/
- bin/         (gitignored)
- obj/         (gitignored)

=== Task 2: Shared headers (include/) ===

Create these headers with EXACTLY the contracts specified in AGENTS.md.
Do not invent new fields. Copy the signatures verbatim.

- include/common.h      → job_t struct, g_queue extern
- include/job_queue.h   → queue_init/push/pop/shutdown/destroy
- include/thread_pool.h → pool_init/shutdown/destroy
- include/server.h      → server_start/server_stop
- include/http.h        → handle_connection
- include/db_wrapper.h  → db_result_t struct, execute_query_safe, db_result_free

All headers must have proper #ifndef guards.

=== Task 3: Empty stub source files ===

For each part, create a .c file under its directory with empty function
bodies that return sensible defaults (0, NULL, or do nothing). This lets
the project compile immediately.

- src/server/server.c
- src/http/http.c
- src/pool/thread_pool.c
- src/pool/db_wrapper.c
- src/queue/job_queue.c
- src/main.c  → a main() that just prints "hello" and exits 0

=== Task 4: Placeholder tests ===

For each test file, add a single trivial test that passes, so `make test`
has something to run before any real implementation lands:

- tests/test_server.c  → assert(1 == 1);
- tests/test_http.c    → assert(1 == 1);
- tests/test_pool.c    → assert(1 == 1);
- tests/test_queue.c   → assert(1 == 1);

Use a minimal test harness (just printf + assert, no external framework)
so teammates can replace them freely.

=== Task 5: Makefile ===

Write a Makefile that:
- Compiles with: gcc -Wall -Wextra -Wpedantic -std=c11 -g -pthread
- Includes -Iinclude -Iexternal/cjson
- Links external/cjson/cJSON.c together with project sources
- Links external/db_engine/*.c together with project sources
- Produces bin/dbms_server from src/*.c + external/
- `make test` builds and runs each tests/test_*.c as a separate binary
- `make clean` removes bin/ and obj/
- Works on both macOS and Linux (no GNU-make-only features where avoidable)

=== Task 6: .gitignore ===

Create .gitignore with the contents from COMMIT_RULES.md (bin/, obj/, *.o,
.vscode/, .DS_Store, etc.).

=== Task 7: Verify ===

Run `make` and `make test`. Both must succeed. If they don't, fix the
Makefile or stubs until they do.

Do NOT implement any real logic in the stubs. That is the 4 teammates' job.
The goal of this setup is: "everything compiles, nothing does anything yet."
```

---

## 3. 커밋 & 푸시

Codex 작업이 끝나면:

```bash
# 결과 확인
make clean && make && make test   # 전부 통과해야 함

# 커밋
git add -A
git commit -m "chore: initial project skeleton with stub contracts"
git push origin main
```

---

## 4. 팀원에게 공지

Slack/카톡에 다음 메시지 전송:

```
✅ main 브랜치에 골격 푸시 완료.
다들 아래 명령으로 각자 브랜치 파고 작업 시작해주세요.

git clone <repo-url>
cd mini-dbms-serve
make && make test   # 다 통과하는지 확인
git checkout -b part<N>-<이름>

각자 AGENTS.md의 파트별 프롬프트를 Codex에 넣으면 됩니다.
공유 헤더(include/*.h) 수정 필요하면 반드시 공지!
```

---

## 5. 완료 체크리스트

- [ ] `AGENTS.md`, `CLAUDE.md`, `COMMIT_RULES.md`, `README.md` 루트에 있음
- [ ] `docs/functional-test-checklist.md`, `docs/edge-cases.md` 배치됨
- [ ] `external/cjson/cJSON.c`, `cJSON.h` 배치됨
- [ ] `external/db_engine/` 에 기존 SQL + B+트리 소스 배치됨
- [ ] `include/` 에 계약 A~E에 해당하는 헤더 6개 존재
- [ ] `src/{server,http,pool,queue}/` 빈 stub 존재
- [ ] `src/main.c` 존재
- [ ] `tests/test_*.c` 4개 플레이스홀더 존재
- [ ] `Makefile`, `.gitignore` 존재
- [ ] `make` 성공 (경고 0개)
- [ ] `make test` 4개 테스트 전부 PASS
- [ ] `main` 브랜치에 푸시 완료
- [ ] 팀에 공지 발송

---

## ⚠️ 주의

- **이 단계에서는 실제 로직을 짜지 않는다.** 로직은 각 팀원이 자기 브랜치에서 작성.
- **DB 엔진 소스가 자체 Makefile을 갖고 있으면 충돌 주의.** 필요시 해당 Makefile은 제외하고 `.c` 파일만 가져와서 루트 Makefile에 흡수.
- **DB 엔진에 `main()` 함수가 있으면 제거하거나 리네임.** 서버 `main()`과 충돌.
- **cJSON은 `NDEBUG` 없이 빌드되면 출력이 많을 수 있다.** 필요시 `-DCJSON_HIDE_SYMBOLS` 옵션 참고.
