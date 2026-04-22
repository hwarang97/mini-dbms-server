/* sql_format.c — 파싱 결과를 다시 SQL 문자열로 되돌리는 부품 (지용)
 * ============================================================================
 *
 * ▣ 이 파일이 하는 일
 *   ParsedSQL 구조체를 보고 "원래 SQL 문장이라면 어떻게 생겼을까?" 를
 *   다시 글자로 만들어낸다. --format 옵션이 켜지면 호출된다.
 *
 * ▣ 왜 필요하지?
 *   1) 입력 SQL 정규화: "create  table  USERS ( id int )" 같이 지저분한
 *      입력도 "CREATE TABLE USERS (id INT);" 처럼 깔끔하게 다시 출력.
 *   2) round-trip 검증: parse → format → parse 결과가 같으면 파서가
 *      정보를 잃지 않고 잘 보존한다는 증명.
 *   3) 발표 임팩트: "이렇게 잘랐고, 다시 합치면 이게 됩니다" 시연.
 *
 * ▣ "정규화" 가 뭐지?
 *   같은 의미의 SQL 을 항상 같은 모양으로 통일하는 것.
 *   - 키워드는 모두 대문자
 *   - 컬럼 사이는 콤마+공백 1칸
 *   - 문자열 값은 작은따옴표
 *   - 끝에 세미콜론
 * ============================================================================
 */

#include "types.h"
#include <stdio.h>
#include <string.h>

/* needs_quote: 이 값이 따옴표로 감싸야 하는 문자열인지 판단.
 *
 *   - 빈 값                          → 따옴표 필요
 *   - 공백/하이픈/콜론 포함         → 따옴표 필요 (예: "hello world", "2024-01-15")
 *   - 첫 글자가 0~9 또는 +/-        → 숫자로 보고 따옴표 안 씀
 *   - 그 외                          → 따옴표 사용 (안전한 기본값)
 */
static int needs_quote(const char *s) {
    if (!s || !*s) return 1;
    for (const char *p = s; *p; p++) {
        if (*p == ' ' || *p == '-' || *p == ':') return 1;
    }
    if ((s[0] >= '0' && s[0] <= '9') || s[0] == '-' || s[0] == '+') return 0;
    return 1;
}

/* emit_value: 값을 적절히 따옴표로 감싸 출력. */
static void emit_value(FILE *out, const char *v) {
    if (needs_quote(v)) fprintf(out, "'%s'", v);
    else                fprintf(out, "%s",   v);
}

static const char *where_link_at(const ParsedSQL *sql, int index) {
    const char *link;

    if (sql == NULL || index < 0) return "";
    if (sql->where_links != NULL) link = sql->where_links[index];
    else if (sql->where_logic[0]) link = sql->where_logic;
    else link = "AND";

    if (link != NULL &&
        (link[0] == 'O' || link[0] == 'o') &&
        (link[1] == 'R' || link[1] == 'r') &&
        link[2] == '\0') return "OR";
    return "AND";
}

/* emit_where: WHERE 절을 출력. WHERE 가 없으면 아무것도 안 함.
 * 두 번째 조건 앞에는 AND/OR 같은 결합 키워드가 들어간다. */
static void emit_where(FILE *out, const ParsedSQL *sql) {
    if (sql->where_count == 0) return;
    fprintf(out, " WHERE ");
    for (int i = 0; i < sql->where_count; i++) {
        if (i > 0) fprintf(out, " %s ", where_link_at(sql, i - 1));
        fprintf(out, "%s %s ", sql->where[i].column, sql->where[i].op);
        emit_value(out, sql->where[i].value);
    }
}

/* print_format: ParsedSQL → 정규화된 SQL 문자열 출력.
 *
 * 쿼리 종류별로 분기해서 각각 다른 모양으로 출력한다. */
void print_format(FILE *out, const ParsedSQL *sql) {
    if (!out || !sql) return;

    switch (sql->type) {

        /* CREATE TABLE name (col1 TYPE1, col2 TYPE2, ...); */
        case QUERY_CREATE: {
            fprintf(out, "CREATE TABLE %s (", sql->table);
            for (int i = 0; i < sql->col_def_count; i++) {
                if (i) fprintf(out, ", ");
                fprintf(out, "%s", sql->col_defs[i]);
            }
            fprintf(out, ");\n");
            break;
        }

        /* INSERT INTO name (col, ...) VALUES (val, ...); */
        case QUERY_INSERT: {
            fprintf(out, "INSERT INTO %s (", sql->table);
            for (int i = 0; i < sql->col_count; i++) {
                if (i) fprintf(out, ", ");
                fprintf(out, "%s", sql->columns[i]);
            }
            fprintf(out, ") VALUES (");
            for (int i = 0; i < sql->val_count; i++) {
                if (i) fprintf(out, ", ");
                emit_value(out, sql->values[i]);
            }
            fprintf(out, ");\n");
            break;
        }

        /* SELECT col, ... FROM name [WHERE ...] [ORDER BY ...] [LIMIT N]; */
        case QUERY_SELECT: {
            fprintf(out, "SELECT ");
            for (int i = 0; i < sql->col_count; i++) {
                if (i) fprintf(out, ", ");
                fprintf(out, "%s", sql->columns[i]);
            }
            fprintf(out, " FROM %s", sql->table);
            emit_where(out, sql);
            if (sql->order_by) {
                fprintf(out, " ORDER BY %s %s",
                        sql->order_by->column,
                        sql->order_by->asc ? "ASC" : "DESC");
            }
            if (sql->limit >= 0) {
                fprintf(out, " LIMIT %d", sql->limit);
            }
            fprintf(out, ";\n");
            break;
        }

        /* DELETE FROM name [WHERE ...]; */
        case QUERY_DELETE: {
            fprintf(out, "DELETE FROM %s", sql->table);
            emit_where(out, sql);
            fprintf(out, ";\n");
            break;
        }

        /* UPDATE name SET col = val, ... [WHERE ...]; */
        case QUERY_UPDATE: {
            fprintf(out, "UPDATE %s SET ", sql->table);
            for (int i = 0; i < sql->set_count; i++) {
                if (i) fprintf(out, ", ");
                fprintf(out, "%s = ", sql->set[i].column);
                emit_value(out, sql->set[i].value);
            }
            emit_where(out, sql);
            fprintf(out, ";\n");
            break;
        }

        /* 알 수 없는 쿼리 종류는 SQL 주석으로 표시. */
        default:
            fprintf(out, "-- (UNKNOWN query type)\n");
            break;
    }
}
