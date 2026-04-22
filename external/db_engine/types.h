/* types.h — 모든 모듈이 공유하는 자료구조와 함수 선언 (지용)
 * ============================================================================
 *
 * ▣ 이 헤더가 하는 일
 *   parser.c, executor.c, storage.c 가 서로 데이터를 주고받을 때 쓰는
 *   "공용 약속" 들이 여기 모두 모여 있다.
 *
 *   - QueryType   : 어떤 종류의 쿼리인지 (SELECT, INSERT, ...)
 *   - ColumnType  : 컬럼 타입 (INT, VARCHAR, ...)
 *   - WhereClause : WHERE 한 조건
 *   - OrderBy     : ORDER BY 정보
 *   - SetClause   : UPDATE SET 한 쌍 (col = val)
 *   - ParsedSQL   : 파싱 결과 통합 구조체 (가장 중요!)
 *
 *   그리고 모든 .c 파일에서 호출 가능한 공개 함수들의 선언.
 *
 * ▣ 주의
 *   storage_* 함수의 시그니처는 절대 변경 금지 (storage.c 주석 참고).
 *   ParsedSQL 구조체에 새 필드를 추가할 때는 반드시 지용 협의.
 * ============================================================================
 */

#ifndef TYPES_H        /* 같은 헤더가 두 번 include 되는 사고 방지 (헤더 가드) */
#define TYPES_H

#include <stdio.h>     /* FILE* 타입을 쓰기 위해 */

/* ─── 쿼리 종류 ─────────────────────────────────────────────────
 * SQL 한 문장이 어떤 종류인지 표시. parse_sql() 이 정해준다.
 * UNKNOWN 은 "지원 안 하는 키워드" 이거나 빈 입력일 때 들어간다.
 */
typedef enum {
    QUERY_SELECT,
    QUERY_INSERT,
    QUERY_DELETE,
    QUERY_UPDATE,
    QUERY_CREATE,
    QUERY_UNKNOWN
} QueryType;

/* ─── 컬럼 타입 ─────────────────────────────────────────────────
 * CREATE TABLE 에서 컬럼이 가질 수 있는 6가지 데이터 타입.
 *
 *   INT       — 정수
 *   VARCHAR   — 가변 길이 문자열
 *   FLOAT     — 실수
 *   BOOLEAN   — 참/거짓
 *   DATE      — 'YYYY-MM-DD' 문자열로 보관 (1주차)
 *   DATETIME  — 1주차에는 파싱만 받고 실제 저장/비교는 2주차로 이관
 */
typedef enum {
    TYPE_INT,
    TYPE_VARCHAR,
    TYPE_FLOAT,
    TYPE_BOOLEAN,
    TYPE_DATE,
    TYPE_DATETIME
} ColumnType;

/* ─── 컬럼 정의 (CREATE TABLE 용) ────────────────────────────── */
typedef struct {
    char       name[64];   /* 컬럼 이름 */
    ColumnType type;       /* 컬럼 타입 */
} ColDef;

/* ─── WHERE 조건 ────────────────────────────────────────────────
 * "age > 20" 같은 한 개 조건을 표현. 여러 개면 ParsedSQL 안에서
 * 배열로 보관하고, 사이에 AND/OR 가 들어간다.
 */
typedef struct {
    char column[64];       /* 비교할 컬럼 이름 (예: "age") */
    char op[8];            /* 비교 연산자  (예: ">", "=", "!=") */
    char value[256];       /* 비교할 값    (예: "20", "alice") */
} WhereClause;

/* ─── ORDER BY ───────────────────────────────────────────────── */
typedef struct {
    char column[64];       /* 정렬 기준 컬럼 */
    int  asc;              /* 1 = ASC (오름차순), 0 = DESC (내림차순) */
} OrderBy;

/* ─── RowSet — Phase 1 신설 ────────────────────────────────────
 * storage 가 SELECT 결과를 메모리 데이터로 반환하기 위한 컨테이너.
 *
 *   1주차: storage_select 가 결과를 stdout 에 print 만 했음 → 다른 코드가
 *          결과를 받아서 쓸 방법이 없었다.
 *   Phase 1: storage_select_result 가 RowSet 으로 반환 → 집계 함수,
 *            JOIN, 향후 subquery 등 모든 데이터 소비자가 활용 가능.
 *
 * 메모리 소유권: storage_select_result 가 만든 RowSet 은 호출자가
 *                rowset_free 로 해제해야 한다.
 */
typedef struct {
    int     row_count;      /* 행 수 */
    int     col_count;      /* 컬럼 수 */
    char  **col_names;      /* 컬럼 이름 배열, 길이 = col_count */
    char ***rows;            /* rows[i][j] = i 번째 행의 j 번째 컬럼 값 (문자열) */
} RowSet;

/* ─── SET (UPDATE 용) ─────────────────────────────────────────
 * "name = 'bob'" 같은 한 쌍을 표현.
 */
typedef struct {
    char column[64];
    char value[256];
} SetClause;

/* ─── ParsedSQL — 파서가 만들어내는 최종 결과물 ──────────────
 *
 * 모든 종류의 쿼리 (SELECT/INSERT/...) 가 이 한 구조체로 표현된다.
 * 쿼리 종류에 따라 일부 필드만 사용한다 (사용 안 하는 필드는 비어있음).
 *
 * 예시:
 *   "SELECT id, name FROM users WHERE age > 20 LIMIT 5"
 *      type        = QUERY_SELECT
 *      table       = "users"
 *      columns     = ["id", "name"], col_count = 2
 *      where       = [{age, >, 20}], where_count = 1
 *      limit       = 5
 *      (나머지 필드는 NULL/0)
 */
typedef struct {
    QueryType    type;             /* 쿼리 종류 */

    char         table[64];        /* 대상 테이블 이름 */

    /* SELECT 의 컬럼 목록, INSERT 의 컬럼 목록 (양쪽 다 같은 자리 사용) */
    char       **columns;
    int          col_count;

    /* INSERT 의 VALUES 목록 */
    char       **values;
    int          val_count;

    /* WHERE 절
     *
     * Phase 1 변경:
     *   1주차: 최대 2개 조건 + 단일 결합자 (where_logic 1개로 표현)
     *   Phase 1: N개 조건 + N-1개 결합자 (where_links 배열)
     *
     * 호환성:
     *   - where_count == 0      : WHERE 없음
     *   - where_count == 1      : 단일 조건 (결합자 없음, where_links == NULL OK)
     *   - where_count == 2 이상 : where_links[0..where_count-2] 가 결합자
     *
     *   1주차 코드 호환을 위해 where_logic 도 유지 (deprecated). 새 코드는
     *   where_links 를 우선 사용하고, where_links == NULL 이면 where_logic
     *   을 모든 결합자로 fallback.
     */
    WhereClause *where;
    int          where_count;       /* 0~N */
    char         where_logic[8];    /* (deprecated) 1주차 호환: 모든 결합자가 동일할 때 */
    char       **where_links;       /* Phase 1: N-1 개의 결합자 ("AND" 또는 "OR")
                                       길이 = where_count - 1, where_count <= 1 이면 NULL */

    /* ORDER BY (없으면 NULL) */
    OrderBy     *order_by;

    /* LIMIT (-1 = 미사용) */
    int          limit;

    /* UPDATE SET 쌍들 */
    SetClause   *set;
    int          set_count;

    /* CREATE TABLE 컬럼 정의 ("id INT", "name VARCHAR" 같은 문자열) */
    char       **col_defs;
    int          col_def_count;

} ParsedSQL;

/* ─── 함수 인터페이스 선언 ───────────────────────────────────
 * 각 .c 파일이 외부에 노출하는 함수들의 약속.
 * 다른 .c 파일은 이 헤더만 include 하면 호출 가능.
 */

/* parser.c */
ParsedSQL *parse_sql(const char *input);
void       free_parsed(ParsedSQL *sql);
void       print_tokens(FILE *out, const char *input);  /* --tokens 플래그용 */

/* ast_print.c — --debug 플래그용 AST 트리 시각화 */
void       print_ast(FILE *out, const ParsedSQL *sql);

/* json_out.c — --json 플래그용 JSON 직렬화 */
void       print_json(FILE *out, const ParsedSQL *sql);

/* sql_format.c — --format 플래그용 정규화 SQL 직렬화 */
void       print_format(FILE *out, const ParsedSQL *sql);

/* executor.c */
void       execute(ParsedSQL *sql);

/* storage.c — 1주차 시그니처 (Phase 1 에서 일부 변경 예정)
 *
 * Phase 1 계획:
 *   - storage_insert / storage_select / storage_create 는 그대로 유지
 *   - storage_delete / storage_update 는 ParsedSQL* 받게 변경
 *     (원우 PR feature/p1-compound-where 에서 본문 + 시그니처 + 호출부 + 테스트
 *      모두 한 번에 수정)
 */
int        storage_insert(const char *table, char **columns, char **values, int count);
int        storage_select(const char *table, ParsedSQL *sql);
int        storage_delete(const char *table, ParsedSQL *sql);
int        storage_update(const char *table, ParsedSQL *sql);
int        storage_create(const char *table, char **col_defs, int count);
int        storage_bulk_begin(const char *table);
int        storage_bulk_end(const char *table);

/* ─── Phase 1 신설 함수 (지용 feature/p1-rowset 에서 본문 구현) ───
 *
 * storage_select_result:
 *   storage_select 와 동일한 입력을 받지만 결과를 RowSet 으로 반환.
 *   호출자가 rowset_free 로 해제해야 한다.
 *   집계 함수 (COUNT/SUM/AVG/MIN/MAX) 도 단일 행 RowSet 으로 반환.
 *
 * print_rowset:
 *   RowSet 을 사람이 읽기 좋은 표 형태로 FILE* 에 출력.
 *   1주차의 storage_select 가 stdout 에 직접 찍던 형태와 동일.
 *
 * rowset_free:
 *   RowSet 과 그 안의 모든 메모리를 재귀 해제. NULL safe.
 */
int  storage_select_result(const char *table, ParsedSQL *sql, RowSet **out);
void print_rowset(FILE *out, const RowSet *rs);
void rowset_free(RowSet *rs);

#endif /* TYPES_H */
