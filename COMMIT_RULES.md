# Git 커밋 규칙 — 미니 DBMS API 서버

PR 없이 파트별 브랜치에서 직접 작업하는 체제입니다.
각자의 책임 영역이 명확하므로 충돌을 최소화하는 데 초점을 맞춥니다.

---

## 브랜치 전략

### 브랜치 구성

```
main                 ← 항상 빌드되고 테스트 통과하는 상태
├── part1-server     ← 석제
├── part2-http       ← 민철
├── part3-pool       ← 주형
└── part4-queue      ← 지운
```

### 규칙

1. **각자 자기 브랜치에서만 작업한다.** 다른 사람 브랜치에 직접 커밋 금지.
2. **`main`에 직접 커밋 금지.** 계약 파일(공유 헤더) 수정이 필요하면 팀 전체 합의 후 한 명이 대표로 `main`에 커밋.
3. **`main`을 자주 pull 받고, 자기 브랜치에 rebase한다.** 하루 2~3회 권장 (점심 전, 통합 직전).
4. **통합(15:00 이후)은 한 명이 main에서 각 브랜치를 순서대로 merge한다.** 순서: queue → pool → http → server.

---

## 커밋 전 체크리스트

매 커밋 전에 반드시 수행:

- [ ] `make` — 빌드 성공
- [ ] `make test` — 전체 테스트 통과
- [ ] 자기 파트 외 파일 수정 여부 확인 (`git diff --stat`)
- [ ] 공유 헤더(`include/*.h`) 수정 여부 확인 — 수정했다면 팀에 공지

**테스트가 하나라도 실패하면 커밋하지 않는다. 예외 없음.**

---

## 커밋 메시지 포맷

### 기본 형식

```
<type>(<part>): <subject>

<body (선택)>
```

### type

| type | 용도 |
|---|---|
| `feat` | 새 기능 추가 |
| `fix` | 버그 수정 |
| `test` | 테스트 추가/수정 |
| `refactor` | 기능 변경 없이 코드 개선 |
| `docs` | 문서 수정 (README, 주석 등) |
| `chore` | 빌드, Makefile, gitignore 등 |
| `wip` | 작업 중간 저장 (로컬 브랜치 한정, merge 전 정리) |

### part

`part1` / `part2` / `part3` / `part4` / `integration` / `shared`

### 예시

```
feat(part1): implement accept loop with SO_REUSEADDR
feat(part2): parse HTTP POST /query and push job to queue
feat(part3): add worker loop with graceful shutdown
feat(part4): implement blocking queue with mutex and condva

fix(part4): broadcast both condvars on shutdown to wake all waiters
test(part3): add stress test for 100 concurrent jobs
refactor(part2): extract header parsing into separate function
docs(shared): update AGENTS.md with client_fd ownership table
chore: add helgrind target to Makefile
```

---

## 커밋 단위

### 작게 자주 커밋한다

- 한 함수 완성 → 커밋
- 테스트 하나 추가 → 커밋
- 버그 하나 수정 → 커밋

### 하나의 커밋에 여러 목적을 섞지 않는다

나쁜 예: `feat(part2): implement HTTP parser and fix queue bug`
좋은 예: 두 개로 분리
```
feat(part2): implement HTTP parse
fix(part4): off-by-one in ring buffer wrap-around
```

### wip 커밋은 로컬에서만

점심 먹으러 갈 때 임시 저장용 `wip:` 커밋은 허용하되, **merge 전에 `git rebase -i`로 정리**한다.

---

## 충돌 방지 룰

### 1. 공유 헤더는 건드리지 않는다

`include/common.h`, `include/job_queue.h`, `include/thread_pool.h`, `include/server.h`, `include/http.h`
이 다섯 파일은 계약이다. **변경이 필요하면 Slack/카톡으로 전원 공지 후 합의**, 한 명이 대표로 `main`에 커밋, 나머지는 `git pull --rebase`로 받는다.

### 2. 파트별 디렉토리에서만 작업

| 담당 | 수정 허용 경로 |
|---|---|
| 석제 | `src/server/`, `tests/test_server.c` |
| 민철 | `src/http/`, `tests/test_http.c` |
| 주형 | `src/pool/`, `tests/test_pool.c` |
| 지운 | `src/queue/`, `tests/test_queue.c` |

Makefile 수정이 필요하면 공지.

### 3. `main.c`는 통합 단계에서만

`src/main.c`는 통합 시점에 전원이 함께 작성. 개별 파트 작업 중엔 손대지 않는다.

---

## 통합 단계 (수요일 15:00 이후)

1. **전원이 자기 브랜치에 최종 커밋 + 푸시**
2. **한 명이 `main`에서 순서대로 merge**
   ```bash
   git checkout main
   git pull
   git merge --no-ff part4-queue
   git merge --no-ff part3-pool
   git merge --no-ff part2-http
   git merge --no-ff part1-serve
   ```
3. **충돌 발생 시** 해당 파트 담당자를 불러 함께 해결
4. **merge 후 `make test` 전체 통과 확인**
5. **`main.c` 작성 → 실제 서버 구동 → curl로 검증**

---

## 커밋 예시 시나리오

### 아침 (10:30~)

```bash
# 석제
git checkout -b part1-serve
# ... accept 루프 구현 ...
make test   # 통과 확인
git add src/server/ tests/test_server.c
git commit -m "feat(part1): implement accept loop with SO_REUSEADDR"
```

### 점심 직전

```bash
git add -A
git commit -m "wip(part1): partial HTTP handoff wiring"
git push origin part1-serve
```

### 점심 후

```bash
git fetch origin
git rebase origin/main        # 공유 헤더 변경사항 반영
# ... 작업 계속 ...
```

### 통합 직전

```bash
git rebase -i HEAD~5          # wip 커밋들 squash
git push --force-with-lease origin part1-serve
```

---

## 금지 사항

- `git push --force` (단, 자기 브랜치의 rebase 후 `--force-with-lease`는 OK)
- `main`에 직접 커밋 (공유 헤더 합의 commit 제외)
- 테스트 실패 상태로 커밋
- 다른 사람 브랜치에 커밋
- `.gitignore` 누락으로 `bin/`, `*.o`, `.vscode/` 등 커밋

---

## .gitignore 기본

```
# Build artifacts
bin/
obj/
*.o
*.a
*.so
*.dSYM/

# Edito
.vscode/
.idea/
*.swp
.DS_Store

# Test output
test-results/
*.log
core
core.*

# Codex / Claude
.codex/
.claude/
```
