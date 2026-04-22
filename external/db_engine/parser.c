/* parser.c — SQL 문장을 컴퓨터가 이해할 수 있는 모양으로 바꾸는 부품 (지용)
 * ============================================================================
 *
 * ▣ 이 파일이 하는 일을 한 줄로 설명하면?
 *   사람이 쓴 SQL 문장 ("SELECT id FROM users WHERE age > 20") 을
 *   컴퓨터가 다루기 쉬운 작은 조각들 (구조체) 로 바꿔주는 일을 한다.
 *
 * ▣ 왜 필요하지?
 *   컴퓨터는 글자 그대로의 문장을 못 알아본다. "SELECT" 가 어디서 시작해서
 *   어디까지가 컬럼 이름인지, 어디부터 WHERE 인지를 일일이 잘라줘야 한다.
 *   이 작업을 "파싱(parsing)" 이라고 부른다.
 *
 * ▣ 두 단계로 동작한다
 *   1) 토크나이저(tokenizer): 문장을 단어 단위로 자른다.
 *      예) "SELECT id FROM t" → ["SELECT", "id", "FROM", "t"]
 *
 *   2) 파서(parser): 자른 단어들을 보고 "이건 SELECT 문장이구나" 알아낸 다음
 *      각 부분 (테이블 이름, 컬럼 목록, WHERE 조건...) 을 ParsedSQL 구조체에
 *      차곡차곡 담는다.
 *
 * ▣ 지원하는 SQL 종류
 *   CREATE TABLE name (col TYPE, ...)
 *   INSERT INTO name (cols) VALUES (vals)
 *   SELECT * | cols FROM name [WHERE ...] [ORDER BY col [ASC|DESC]] [LIMIT n]
 *   DELETE FROM name [WHERE ...]
 *   UPDATE name SET col=val[, ...] [WHERE ...]
 *
 *   WHERE 는 1~2개 조건 + AND/OR 결합까지 지원.
 *   대소문자 구분 안 함 (select 와 SELECT 같음).
 * ============================================================================
 */

#define _POSIX_C_SOURCE 200809L  /* strdup() 같은 POSIX 함수를 쓰기 위한 매직 줄 */

#include "types.h"
#include <stdio.h>     /* fprintf, FILE  */
#include <stdlib.h>    /* malloc, free   */
#include <string.h>    /* strcmp, memcpy */
#include <ctype.h>     /* isalnum, tolower, isspace */

/* ============================================================================
 * 1단계: 토크나이저
 *   SQL 문자열 → 단어(토큰) 배열로 자르기
 * ============================================================================
 */

/* TokenList: 잘라낸 토큰들을 담아두는 가방 같은 구조체.
 *
 *   tok    — 토큰 문자열들이 줄지어 있는 배열 (예: tok[0]="SELECT", tok[1]="id"...)
 *   count  — 지금까지 몇 개 담았는지
 *   cap    — 가방이 최대 몇 개까지 담을 수 있는지 (꽉 차면 두 배로 늘림)
 *   pos    — 파서가 토큰을 읽어가는 "현재 위치" 표시 (책갈피라고 생각)
 */
typedef struct {
    char **tok;
    int    count;
    int    cap;
    int    pos;
} TokenList;

/* 빈 TokenList 를 새로 만들기. 처음엔 16칸짜리 가방으로 시작. */
static TokenList *tl_new(void) {
    TokenList *t = calloc(1, sizeof(*t));   /* calloc → 메모리 할당 + 0으로 초기화 */
    t->cap = 16;
    t->tok = calloc(t->cap, sizeof(char *));
    return t;
}

/* TokenList 에 토큰 하나 추가하기.
 * 가방이 꽉 차면 두 배로 늘려서 자리를 확보한 다음 넣는다. */
static void tl_push(TokenList *t, const char *s, int len) {
    if (t->count >= t->cap) {
        t->cap *= 2;                                      /* 가방 두 배로 */
        t->tok = realloc(t->tok, t->cap * sizeof(char *));
    }
    /* 원본 문자열을 그대로 가리키면 위험하니까 (원본이 사라질 수 있음)
     * 토큰만큼만 새로 메모리에 복사해 둔다. */
    char *dup = malloc(len + 1);   /* +1 은 문자열 끝 '\0' 자리 */
    memcpy(dup, s, len);
    dup[len] = '\0';
    t->tok[t->count++] = dup;
}

/* TokenList 와 그 안의 토큰들을 모두 메모리에서 해제. */
static void tl_free(TokenList *t) {
    if (!t) return;
    for (int i = 0; i < t->count; i++) free(t->tok[i]);
    free(t->tok);
    free(t);
}

/* 토크나이저 본체: SQL 문자열을 한 글자씩 보면서 토큰으로 잘라낸다.
 *
 * 동작 방식: 포인터 p 가 문자열을 한 글자씩 앞으로 이동하면서,
 * "지금 보고 있는 글자가 어떤 종류인가?" 에 따라 다른 처리를 한다.
 *
 *   - 공백/탭/줄바꿈    → 그냥 건너뜀
 *   - --                → SQL 한 줄 주석. 줄 끝까지 건너뜀
 *   - ' 또는 "          → 따옴표 안 내용 통째로 한 토큰
 *   - , ( ) ; *         → 1글자짜리 단독 토큰
 *   - = > < ! 등        → 비교 연산자 (1~2글자)
 *   - 글자/숫자/_/./-   → 식별자나 숫자 (예: id, 42, 3.14)
 */
static TokenList *tokenize(const char *input) {
    TokenList *t = tl_new();
    const char *p = input;

    while (*p) {
        /* (1) 공백류는 무조건 건너뛴다. */
        if (isspace((unsigned char)*p)) { p++; continue; }

        /* (2) "--" 로 시작하면 그 줄 끝(\n) 까지 모두 무시. SQL 라인 주석. */
        if (*p == '-' && *(p + 1) == '-') {
            while (*p && *p != '\n') p++;
            continue;
        }

        /* (3) 따옴표 문자열 처리.
         *     'hello world' → 그대로 한 토큰 ("hello world").
         *     따옴표 안에서는 공백도 콤마도 모두 글자로 취급. */
        if (*p == '\'' || *p == '"') {
            char quote = *p++;                /* 어떤 따옴표인지 기억하고 한 칸 전진 */
            const char *start = p;            /* 내용 시작 위치 */
            while (*p && *p != quote) p++;    /* 닫는 따옴표까지 이동 */
            tl_push(t, start, (int)(p - start));
            if (*p == quote) p++;             /* 닫는 따옴표도 건너뜀 */
            continue;
        }

        /* (4) 1글자짜리 특수문자는 그 자체로 한 토큰. */
        if (*p == ',' || *p == '(' || *p == ')' || *p == ';' || *p == '*') {
            tl_push(t, p, 1);
            p++;
            continue;
        }

        /* (5) 비교 연산자.
         *     '=' 는 단독 1글자.
         *     '>' '<' '!' 는 뒤에 '=' 가 붙으면 2글자 (>=, <=, !=). */
        if (*p == '=') {
            tl_push(t, p, 1);
            p++;
            continue;
        }
        if (*p == '>' || *p == '<' || *p == '!') {
            if (*(p + 1) == '=') {
                tl_push(t, p, 2);   /* 두 글자짜리 연산자 */
                p += 2;
            } else {
                tl_push(t, p, 1);   /* 한 글자짜리 */
                p++;
            }
            continue;
        }

        /* (6) 식별자 / 키워드 / 숫자.
         *     글자, 숫자, _, ., - 가 이어지는 동안 한 토큰으로 묶는다.
         *     예: "users", "user_id", "3.14", "-5", "2024-01-15" */
        if (isalnum((unsigned char)*p) || *p == '_' || *p == '.' || *p == '-') {
            const char *start = p;
            while (*p && (isalnum((unsigned char)*p) || *p == '_' || *p == '.' || *p == '-'))
                p++;
            tl_push(t, start, (int)(p - start));
            continue;
        }

        /* (7) 위 어디에도 안 걸리는 글자는 그냥 건너뜀 (안전장치). */
        p++;
    }

    return t;
}

/* ============================================================================
 * 2단계: 파서 보조 함수들
 *   토큰 배열을 앞에서부터 하나씩 꺼내며 "지금 키워드 나왔나?" 를 확인.
 * ============================================================================
 */

/* ieq: 두 문자열이 대소문자 무시하고 같은지 비교. ("SELECT" 와 "select" 같음) */
static int ieq(const char *a, const char *b) {
    if (!a || !b) return 0;
    while (*a && *b) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return 0;
        a++; b++;
    }
    return *a == '\0' && *b == '\0';
}

/* peek: 현재 위치의 토큰을 "보기만" 하고 위치는 안 옮김. */
static const char *peek(TokenList *t) {
    return t->pos < t->count ? t->tok[t->pos] : NULL;
}

/* advance: 현재 토큰을 꺼내고 위치를 한 칸 앞으로. (책 한 페이지 넘기기) */
static const char *advance(TokenList *t) {
    return t->pos < t->count ? t->tok[t->pos++] : NULL;
}

/* match: 현재 토큰이 기대하는 키워드면 위치를 옮기고 1 반환, 아니면 0 반환.
 *        (예: "WHERE 가 있으면 다음으로 가, 없으면 그냥 둬") */
static int match(TokenList *t, const char *kw) {
    if (peek(t) && ieq(peek(t), kw)) { t->pos++; return 1; }
    return 0;
}

/* expect: match 와 비슷한데, 키워드가 없으면 에러 메시지를 stderr 에 찍고 0 반환.
 *         "이 자리에 반드시 이 키워드가 있어야 해" 라고 강제할 때 쓴다. */
static int expect(TokenList *t, const char *kw) {
    if (!match(t, kw)) {
        fprintf(stderr, "[parser] expected '%s', got '%s'\n",
                kw, peek(t) ? peek(t) : "<eof>");
        return 0;
    }
    return 1;
}

/* ============================================================================
 * 3단계: ColumnType 검증 + ParsedSQL 메모리 관리
 * ============================================================================
 */

/* CREATE TABLE 에서 컬럼 타입으로 쓸 수 있는 6가지 중 하나인지 확인. */
static int is_valid_type(const char *s) {
    return ieq(s, "INT") || ieq(s, "VARCHAR") || ieq(s, "FLOAT") ||
           ieq(s, "BOOLEAN") || ieq(s, "DATE") || ieq(s, "DATETIME");
}

/* 빈 ParsedSQL 구조체를 새로 만들기.
 * type 은 일단 UNKNOWN, limit 는 -1 (LIMIT 없음 표시) 로 초기화. */
static ParsedSQL *parsed_new(void) {
    ParsedSQL *p = calloc(1, sizeof(*p));
    p->type = QUERY_UNKNOWN;
    p->limit = -1;
    return p;
}

/* free_parsed: ParsedSQL 안에 들어있는 모든 메모리를 정리.
 *
 * C 언어는 자동 청소 (가비지 컬렉션) 가 없어서, malloc 한 메모리는
 * 반드시 free 로 돌려줘야 한다. 안 그러면 메모리 누수.
 *
 * ParsedSQL 안에는 char** 처럼 "포인터의 배열" 이 여러 개 있다.
 * 각각에 대해 (a) 안에 있는 문자열들을 먼저 free, (b) 그 다음 배열 자체를 free.
 * 마지막으로 ParsedSQL 자체를 free. */
void free_parsed(ParsedSQL *sql) {
    if (!sql) return;
    if (sql->columns) {
        for (int i = 0; i < sql->col_count; i++) free(sql->columns[i]);
        free(sql->columns);
    }
    if (sql->values) {
        for (int i = 0; i < sql->val_count; i++) free(sql->values[i]);
        free(sql->values);
    }
    if (sql->col_defs) {
        for (int i = 0; i < sql->col_def_count; i++) free(sql->col_defs[i]);
        free(sql->col_defs);
    }
    free(sql->where);     /* WhereClause 배열 (안에 동적 메모리 없음) */
    /* Phase 1: where_links — N-1 개의 결합자 문자열 배열.
     * 1주차 parser 는 이 필드를 안 쓰므로 NULL. 향후 N-ary 파서가 strdup 으로
     * 채울 때 free 가 동작해야 한다. */
    if (sql->where_links) {
        for (int i = 0; i + 1 < sql->where_count; i++) free(sql->where_links[i]);
        free(sql->where_links);
    }
    free(sql->set);       /* SetClause 배열   (안에 동적 메모리 없음) */
    free(sql->order_by);  /* OrderBy 1개      */
    free(sql);
}

/* parse_ident_list: 괄호 안의 콤마 구분 식별자 목록을 배열로 만든다.
 *
 * 예) (id, name, age)  →  arr = ["id", "name", "age"]
 *
 *  - 호출 직전에 '(' 는 이미 소비되어 있어야 한다.
 *  - ')' 를 만나면 종료하고 그것까지 소비.
 *  - out_count 에 몇 개 모았는지 돌려준다. */
static char **parse_ident_list(TokenList *t, int *out_count) {
    char **arr = NULL;
    int    n = 0, cap = 0;
    while (peek(t) && strcmp(peek(t), ")") != 0) {
        if (n >= cap) {
            cap = cap ? cap * 2 : 4;        /* 처음엔 4칸, 부족하면 2배씩 */
            arr = realloc(arr, cap * sizeof(char *));
        }
        arr[n++] = strdup(advance(t));      /* 토큰 문자열을 복사해서 저장 */
        if (!match(t, ",")) break;          /* 콤마가 없으면 끝 */
    }
    expect(t, ")");                          /* 마지막에 ')' 강제 */
    *out_count = n;
    return arr;
}

/* SELECT 컬럼 목록을 읽다가 멈춰야 하는 경계 토큰들.
 * 정상 종료 지점인 FROM 외에도, 미래의 괄호/서브쿼리 문맥을 위해
 * 경계 밖 토큰을 컬럼으로 삼키지 않도록 stop set 을 둔다. */
static int is_select_stop_token(const char *tok) {
    if (!tok) return 1;
    return ieq(tok, "FROM")  || ieq(tok, "WHERE") || ieq(tok, "ORDER") ||
           ieq(tok, "LIMIT") || strcmp(tok, ")") == 0 || strcmp(tok, ";") == 0;
}

/* ============================================================================
 * 4단계: WHERE 절 파서
 *   "WHERE col op value [AND/OR col op value]..." 를 N개까지 읽는다.
 * ============================================================================
 */

/* parse_where: 호출 시점에 'WHERE' 키워드는 이미 소비되어 있다고 가정.
 *
 *   WHERE age > 20                              → 조건 1개
 *   WHERE age > 20 AND name = 'bob'             → 조건 2개 + 결합 1개
 *   WHERE age > 20 AND name = 'bob' OR city = 'Seoul'
 *                                               → 조건 3개 + 결합 2개
 *
 * Phase 1 에서는 괄호 그룹화 없이 왼쪽에서 오른쪽 순서로 평면 N-ary 구조만 만든다. */
static void parse_where(TokenList *t, ParsedSQL *sql) {
    int where_cap = 0;
    int links_cap = 0;
    int link_count = 0;

    sql->where = NULL;
    sql->where_count = 0;
    sql->where_links = NULL;
    sql->where_logic[0] = '\0';

    while (1) {
        const char *col = advance(t);   /* 컬럼 이름 (예: age)   */
        const char *op  = advance(t);   /* 연산자  (예: >)       */
        const char *val = advance(t);   /* 값      (예: 20)      */
        if (!col || !op || !val) break;

        if (sql->where_count >= where_cap) {
            where_cap = where_cap ? where_cap * 2 : 4;
            sql->where = realloc(sql->where, where_cap * sizeof(WhereClause));
        }

        WhereClause *w = &sql->where[sql->where_count++];
        strncpy(w->column, col, sizeof(w->column) - 1);
        strncpy(w->op,     op,  sizeof(w->op) - 1);
        strncpy(w->value,  val, sizeof(w->value) - 1);

        /* 다음 토큰이 AND/OR 면 결합자를 저장하고 다음 조건을 계속 읽는다. */
        if (!peek(t) || (!ieq(peek(t), "AND") && !ieq(peek(t), "OR"))) {
            break;
        }

        if (link_count >= links_cap) {
            links_cap = links_cap ? links_cap * 2 : 4;
            sql->where_links = realloc(sql->where_links, links_cap * sizeof(char *));
        }
        {
            const char *logic = advance(t);
            sql->where_links[link_count++] = strdup(ieq(logic, "OR") ? "OR" : "AND");
        }
    }

    /* 모든 결합자가 동일할 때만 1주차 호환 필드를 채운다. 혼합이면 빈 값 유지. */
    if (link_count > 0 && link_count != sql->where_count - 1) {
        free(sql->where_links[--link_count]);
    }

    if (link_count == 0 && sql->where_links) {
        free(sql->where_links);
        sql->where_links = NULL;
    }

    if (link_count > 0 && sql->where_links) {
        int same_logic = 1;
        for (int i = 1; i < link_count; i++) {
            if (!ieq(sql->where_links[0], sql->where_links[i])) {
                same_logic = 0;
                break;
            }
        }
        if (same_logic) {
            strncpy(sql->where_logic, sql->where_links[0], sizeof(sql->where_logic) - 1);
        }
    }
}

/* ============================================================================
 * 5단계: 쿼리 종류별 파서
 *   각 SQL 종류마다 "이런 모양이어야 한다" 는 규칙을 코드로 옮긴 것.
 *   호출 시점에 첫 키워드 (CREATE, INSERT 등) 는 이미 소비된 상태.
 * ============================================================================
 */

/* CREATE TABLE name (col TYPE, col TYPE, ...) */
static void parse_create(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_CREATE;
    if (!expect(t, "TABLE")) return;        /* CREATE 다음에 TABLE 강제 */

    const char *name = advance(t);          /* 테이블 이름 */
    if (!name) { fprintf(stderr, "[parser] CREATE: missing table name\n"); return; }
    strncpy(sql->table, name, sizeof(sql->table) - 1);

    if (!expect(t, "(")) return;            /* 컬럼 목록 시작 */

    int cap = 4;
    sql->col_defs = calloc(cap, sizeof(char *));

    /* "이름 타입, 이름 타입, ...)" 형태를 끝까지 읽는다. */
    while (peek(t) && strcmp(peek(t), ")") != 0) {
        const char *cname = advance(t);     /* 컬럼 이름 */
        const char *ctype = advance(t);     /* 컬럼 타입 */
        if (!cname || !ctype) break;

        /* 알려진 타입이 아니면 경고만 찍고 계속 진행 (실수 알려주기). */
        if (!is_valid_type(ctype)) {
            fprintf(stderr, "[parser] CREATE: invalid type '%s' for column '%s'\n",
                    ctype, cname);
        }

        /* 배열 자리가 부족하면 두 배로 늘리기. */
        if (sql->col_def_count >= cap) {
            cap *= 2;
            sql->col_defs = realloc(sql->col_defs, cap * sizeof(char *));
        }
        /* "id INT" 처럼 한 문자열로 합쳐서 저장. */
        char buf[160];
        snprintf(buf, sizeof(buf), "%s %s", cname, ctype);
        sql->col_defs[sql->col_def_count++] = strdup(buf);

        if (!match(t, ",")) break;          /* 콤마 없으면 마지막 */
    }
    expect(t, ")");
}

/* ─── --tokens 플래그용: 토큰만 출력하는 외부 공개 함수 ────── */
/*
 * 파싱은 안 하고 "토크나이저가 어떻게 잘랐는지" 만 보여준다.
 * 발표/디버깅 시 토크나이저 단계 단독 확인에 유용.
 */
void print_tokens(FILE *out, const char *input) {
    if (!out || !input) return;
    TokenList *t = tokenize(input);
    fprintf(out, "tokens (%d):\n", t->count);
    for (int i = 0; i < t->count; i++) {
        fprintf(out, "  [%2d] %s\n", i, t->tok[i]);
    }
    tl_free(t);
}

/* INSERT INTO name (col, col, ...) VALUES (val, val, ...) */
static void parse_insert(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_INSERT;
    if (!expect(t, "INTO")) return;         /* INSERT 다음 INTO */

    const char *name = advance(t);
    if (!name) { fprintf(stderr, "[parser] INSERT: missing table name\n"); return; }
    strncpy(sql->table, name, sizeof(sql->table) - 1);

    if (!expect(t, "(")) return;
    sql->columns = parse_ident_list(t, &sql->col_count);   /* 컬럼 이름들 */

    if (!expect(t, "VALUES")) return;
    if (!expect(t, "(")) return;
    sql->values = parse_ident_list(t, &sql->val_count);    /* 값들 */
}

/* SELECT col1, col2 | * FROM name [WHERE ...] [ORDER BY ...] [LIMIT N]
 *
 * 컬럼 자리에는 일반 식별자 외에 함수 호출형 (`COUNT(*)`, `SUM(price)` 등) 이
 * 올 수 있다. 토크나이저는 `COUNT`, `(`, `*`, `)` 4개 토큰으로 자르므로
 * 여기서 다음 토큰이 `(` 면 닫는 `)` 까지 이어붙여 한 컬럼 문자열로 만든다. */
static void parse_select(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_SELECT;

    /* 컬럼 목록 (혹은 *).
     * FROM 이 정상 종료 지점이고, stop set 은 경계 밖 토큰을 컬럼으로
     * 잘못 먹지 않도록 막아준다. */
    int cap = 4;
    sql->columns = calloc(cap, sizeof(char *));
    while (peek(t) && !is_select_stop_token(peek(t))) {
        if (sql->col_count >= cap) {
            cap *= 2;
            sql->columns = realloc(sql->columns, cap * sizeof(char *));
        }

        /* 컬럼 이름 한 토큰 읽기. */
        const char *first = advance(t);
        if (!first) break;

        char colbuf[160];
        size_t pos = 0;
        size_t flen = strlen(first);
        if (flen >= sizeof(colbuf)) flen = sizeof(colbuf) - 1;
        memcpy(colbuf, first, flen);
        pos = flen;
        colbuf[pos] = '\0';

        /* 다음 토큰이 '(' 면 함수 호출형 (COUNT(*), SUM(x) 등).
         * 닫는 ')' 까지 모든 토큰을 순서대로 이어붙인다.
         * 세미콜론을 만나면 거기서 멈춰 경계 밖 토큰을 먹지 않는다. */
        if (peek(t) && strcmp(peek(t), "(") == 0) {
            advance(t);  /* '(' 소비 */
            if (pos < sizeof(colbuf) - 1) colbuf[pos++] = '(';
            while (peek(t) && strcmp(peek(t), ")") != 0 && strcmp(peek(t), ";") != 0) {
                const char *inner = advance(t);
                size_t ilen = strlen(inner);
                if (pos + ilen >= sizeof(colbuf) - 1) ilen = sizeof(colbuf) - 1 - pos;
                memcpy(colbuf + pos, inner, ilen);
                pos += ilen;
            }
            if (peek(t) && strcmp(peek(t), ")") == 0) {
                advance(t);  /* ')' 소비 */
                if (pos < sizeof(colbuf) - 1) colbuf[pos++] = ')';
            }
            colbuf[pos] = '\0';
        }

        sql->columns[sql->col_count++] = strdup(colbuf);
        if (!match(t, ",")) break;
    }

    if (!expect(t, "FROM")) return;
    const char *name = advance(t);          /* 테이블 이름 */
    if (name) strncpy(sql->table, name, sizeof(sql->table) - 1);

    /* WHERE 가 있으면 조건 읽기. */
    if (match(t, "WHERE")) parse_where(t, sql);

    /* ORDER BY col [ASC|DESC] */
    if (match(t, "ORDER")) {
        expect(t, "BY");
        sql->order_by = calloc(1, sizeof(OrderBy));
        const char *col = advance(t);
        if (col) strncpy(sql->order_by->column, col, sizeof(sql->order_by->column) - 1);
        sql->order_by->asc = 1;             /* 기본 ASC */
        if (match(t, "DESC")) sql->order_by->asc = 0;
        else                  match(t, "ASC");  /* 명시 ASC 도 소비 */
    }

    /* LIMIT N */
    if (match(t, "LIMIT")) {
        const char *n = advance(t);
        if (n) sql->limit = atoi(n);
    }
}

/* DELETE FROM name [WHERE ...] */
static void parse_delete(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_DELETE;
    if (!expect(t, "FROM")) return;
    const char *name = advance(t);
    if (name) strncpy(sql->table, name, sizeof(sql->table) - 1);
    if (match(t, "WHERE")) parse_where(t, sql);
}

/* UPDATE name SET col = val [, col = val ...] [WHERE ...] */
static void parse_update(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_UPDATE;
    const char *name = advance(t);          /* 테이블 이름 */
    if (name) strncpy(sql->table, name, sizeof(sql->table) - 1);

    if (!expect(t, "SET")) return;

    /* SET col = val 쌍을 콤마로 구분해 끝까지 읽는다. */
    int cap = 4;
    sql->set = calloc(cap, sizeof(SetClause));
    while (peek(t) && !ieq(peek(t), "WHERE") && strcmp(peek(t), ";") != 0) {
        if (sql->set_count >= cap) {
            cap *= 2;
            sql->set = realloc(sql->set, cap * sizeof(SetClause));
        }
        const char *col = advance(t);       /* 컬럼 이름 */
        const char *eq  = advance(t);       /* '=' 토큰 (필요 없으니 (void) 처리) */
        const char *val = advance(t);       /* 새 값 */
        (void)eq;
        if (!col || !val) break;

        SetClause *s = &sql->set[sql->set_count++];
        strncpy(s->column, col, sizeof(s->column) - 1);
        strncpy(s->value,  val, sizeof(s->value) - 1);

        if (!match(t, ",")) break;          /* 콤마 없으면 마지막 SET */
    }

    /* SET 다음에 WHERE 가 올 수도 있다. */
    if (match(t, "WHERE")) parse_where(t, sql);
}

/* ============================================================================
 * 6단계: 진입점 — parse_sql
 *   외부에서 호출하는 유일한 공개 함수.
 *   "이 SQL 문장 좀 분석해줘" → ParsedSQL 구조체로 돌려준다.
 * ============================================================================
 */

ParsedSQL *parse_sql(const char *input) {
    if (!input) return NULL;

    /* 1) 문자열을 토큰들로 자른다. */
    TokenList *t = tokenize(input);
    if (t->count == 0) { tl_free(t); return NULL; }   /* 빈 입력 처리 */

    /* 2) 빈 ParsedSQL 을 만들고, 첫 토큰 (=쿼리 종류) 을 본다. */
    ParsedSQL *sql = parsed_new();
    const char *kw = advance(t);

    /* 3) 종류에 맞는 파서로 보낸다. */
    if      (ieq(kw, "CREATE")) parse_create(t, sql);
    else if (ieq(kw, "INSERT")) parse_insert(t, sql);
    else if (ieq(kw, "SELECT")) parse_select(t, sql);
    else if (ieq(kw, "DELETE")) parse_delete(t, sql);
    else if (ieq(kw, "UPDATE")) parse_update(t, sql);
    else {
        fprintf(stderr, "[parser] unknown keyword: %s\n", kw ? kw : "<eof>");
        sql->type = QUERY_UNKNOWN;
    }

    /* 4) 토큰 가방은 더 이상 필요 없으니 정리하고, 결과만 돌려준다. */
    tl_free(t);
    return sql;
}
