# 미니 DBMS API 서버

> C 언어로 구현한 멀티스레드 DBMS API 서버
> 정글 5기 · 수요 코딩회 · 2026.XX.XX

---

## 📋 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [주요 기능](#2-주요-기능)
3. [아키텍처](#3-아키텍처)
4. [기술 스택](#4-기술-스택)
5. [빌드 및 실행](#5-빌드-및-실행)
6. [API 명세](#6-api-명세)
7. [핵심 설계 결정](#7-핵심-설계-결정)
8. [동시성 제어 전략](#8-동시성-제어-전략)
9. [테스트](#9-테스트)
10. [팀 구성 및 역할](#10-팀-구성-및-역할)
11. [트러블슈팅](#11-트러블슈팅)
12. [회고](#12-회고)

---

## 1. 프로젝트 개요

<!-- 무엇을, 왜 만들었는지. 2~3문장 -->

---

## 2. 주요 기능

<!-- 불릿 4~6개 -->
<!-- - HTTP 기반 SQL 쿼리 API -->
<!-- - Thread Pool 기반 병렬 처리 -->
<!-- - B+ 트리 인덱스 활용 -->
<!-- - ... -->

---

## 3. 아키텍처

### 시스템 구성도

```
[클라이언트]
    ↓ HTTP
[Part 1: 소켓 서버]
    ↓ client_fd
[Part 2: HTTP 파서]
    ↓ job_t (client_fd, sql)
[Part 4: Job Queue]  ←→  [Part 3: Thread Pool]
                              ↓ execute_query()
                         [DB 엔진 (SQL 처리기 + B+ 트리)]
```

### 요청 처리 흐름

<!-- 1. 클라이언트가 HTTP POST /query 전송 -->
<!-- 2. 소켓 서버가 accept -->
<!-- 3. ... -->

---

## 4. 기술 스택

<!-- C / pthread / Make / ... -->

---

## 5. 빌드 및 실행

### 요구 환경

- macOS / Linux (WSL)
- gcc or clang
- pthread

### 빌드

```bash
make
```

### 실행

```bash
./bin/dbms_server 8080
```

### 테스트

```bash
make test
```

---

## 6. API 명세

### POST /query

SQL 쿼리 실행

**Request**
```json
{ "sql": "SELECT * FROM users" }
```

**Response (200)**
```json
{ "rows": [...], "count": N }
```

**Errors**
- `400` — 잘못된 JSON / 빈 SQL
- `500` — 내부 실행 오류

### GET /health

<!-- ... -->

### GET /tables

<!-- ... -->

---

## 7. 핵심 설계 결정

### 7.1 왜 Thread Pool인가

<!-- accept마다 스레드 생성 vs 풀 방식 비교, 선택 이유 -->

### 7.2 Job Queue 자료구조 선택

<!-- ring buffer vs linked list -->

### 7.3 client_fd 소유권 이동 규칙

<!-- accept → parse → worker 로 넘어갈 때 누가 close 책임지는지 -->

---

## 8. 동시성 제어 전략

### 8.1 Queue 동기화

<!-- mutex + condvar (not_empty, not_full) 구조 설명 -->

### 8.2 DB 엔진 보호

<!-- Reader-Writer Lock 적용 이유와 Lock Granularity 결정 -->

### 8.3 Graceful Shutdown

<!-- queue_shutdown → NULL 반환으로 worker 종료 신호 전파 -->

---

## 9. 테스트

### 단위 테스트

<!-- 파트별 테스트 항목 요약 -->

### 기능 테스트 (API 레벨)

<!-- curl 기반 시나리오 — functional-test-checklist.md 참고 -->

### 동시성 테스트

<!-- 부하 테스트 결과, helgrind / ThreadSanitizer 결과 -->

---

## 10. 팀 구성 및 역할

| Part | 담당 | 범위 |
|---|---|---|
| 1. 네트워크 서버 뼈대 | 석제 | socket / bind / listen / accept |
| 2. HTTP 프로토콜 | 민철 | request 파싱 / response 작성 |
| 3. 스레드 운영 | 주형 | pool init / worker loop / shutdown |
| 4. 큐 + 동기화 | 지운 | enqueue / dequeue / mutex-condvar |

---

## 11. 트러블슈팅

<!-- 통합 중 겪은 버그와 해결 과정. 발표 때 임팩트 큰 섹션 -->

### 11.1 (예시) 종료 시 worker가 깨어나지 않음

<!-- 문제 → 원인 → 해결 -->

### 11.2 (예시) client_fd 이중 close

<!-- -->

---

## 12. 회고

<!-- AI 활용 경험, 다음에 개선할 점 -->

---

## 📎 관련 문서

- [AGENTS.md](./AGENTS.md) — Codex CLI 작업 규칙
- [COMMIT_RULES.md](./COMMIT_RULES.md) — Git 커밋 규칙
- [functional-test-checklist.md](./docs/functional-test-checklist.md) — 기능 테스트 체크리스트
- [edge-cases.md](./docs/edge-cases.md) — 엣지 케이스 목록
