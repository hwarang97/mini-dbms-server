/* json_out.c — 파싱 결과를 JSON 글자로 바꾸는 부품 (지용)
 * ============================================================================
 *
 * ▣ 이 파일이 하는 일
 *   ParsedSQL 구조체를 JSON 형식의 문자열로 출력한다.
 *   --json 옵션을 켜면 이 함수가 호출된다.
 *
 * ▣ JSON 이 뭐지?
 *   JavaScript Object Notation. 데이터를 사람도 컴퓨터도 읽기 쉬운
 *   글자 형식으로 표현하는 표준. 객체는 {} 로, 배열은 [] 로,
 *   문자열은 "" 로 감싼다.
 *
 *   예) {"type":"SELECT","table":"users","columns":["id","name"]}
 *
 * ▣ 왜 JSON 으로?
 *   1) Python 중계 서버 (server.py) 가 받아서 그대로 HTML 뷰어로 전달 가능
 *   2) 다른 프로그램이 우리 파서 결과를 받아 쓸 수 있음
 *   3) 디버그할 때도 보기 좋음
 *
 * ▣ 이스케이프 처리
 *   JSON 에서 문자열 안에 " 가 들어가려면 \" 처럼 백슬래시로 표시해야 한다.
 *   백슬래시 자체도 \\ 로 표시해야 한다.
 *   그 외 글자는 그대로 통과.
 * ============================================================================
 */

#include "types.h"
#include <stdio.h>
#include <string.h>

/* QueryType enum → JSON 에 들어갈 문자열. */
static const char *qtype_str(QueryType t) {
    switch (t) {
        case QUERY_SELECT:  return "SELECT";
        case QUERY_INSERT:  return "INSERT";
        case QUERY_DELETE:  return "DELETE";
        case QUERY_UPDATE:  return "UPDATE";
        case QUERY_CREATE:  return "CREATE";
        default:            return "UNKNOWN";
    }
}

/* emit_str: 문자열 하나를 JSON 식 "..." 으로 출력.
 * 안에 " 나 \ 가 있으면 앞에 \ 를 붙여 이스케이프. */
static void emit_str(FILE *out, const char *s) {
    fputc('"', out);
    if (s) {
        for (; *s; s++) {
            if (*s == '"' || *s == '\\') fputc('\\', out);
            fputc(*s, out);
        }
    }
    fputc('"', out);
}

/* emit_str_array: 문자열 배열을 JSON 배열 ["a","b","c"] 형태로 출력.
 * 콤마 위치 처리: 첫 항목 앞에는 콤마 없음. */
static void emit_str_array(FILE *out, char **arr, int n) {
    fputc('[', out);
    for (int i = 0; i < n; i++) {
        if (i) fputc(',', out);
        emit_str(out, arr[i]);
    }
    fputc(']', out);
}

/* condition_index 번째 WHERE 조건 앞에 붙는 결합자.
 * Phase 1 에서는 where_links 를 우선 쓰고, 1주차 호환을 위해 where_logic 으로
 * fallback 한다. */
static const char *where_link_at(const ParsedSQL *sql, int condition_index) {
    if (!sql || condition_index <= 0) return NULL;
    if (sql->where_links && sql->where_links[condition_index - 1]) {
        return sql->where_links[condition_index - 1];
    }
    if (sql->where_logic[0]) return sql->where_logic;
    return NULL;
}

/* print_json: ParsedSQL 을 통째로 JSON 으로 출력.
 *
 * 큰 그림은 print_ast 와 같다. 비어있는 필드는 키 자체를 생략.
 * 마지막에 줄바꿈 1개를 찍어서 여러 statement 가 한 줄씩 떨어지게 한다. */
void print_json(FILE *out, const ParsedSQL *sql) {
    if (!out || !sql) return;

    fputc('{', out);

    /* 항상 들어가는 두 키: type, table */
    fprintf(out, "\"type\":");        emit_str(out, qtype_str(sql->type));
    fprintf(out, ",\"table\":");      emit_str(out, sql->table);

    /* SELECT/INSERT 컬럼 목록 */
    if (sql->col_count > 0) {
        fprintf(out, ",\"columns\":");
        emit_str_array(out, sql->columns, sql->col_count);
    }

    /* INSERT 값 목록 */
    if (sql->val_count > 0) {
        fprintf(out, ",\"values\":");
        emit_str_array(out, sql->values, sql->val_count);
    }

    /* CREATE TABLE 컬럼 정의 */
    if (sql->col_def_count > 0) {
        fprintf(out, ",\"col_defs\":");
        emit_str_array(out, sql->col_defs, sql->col_def_count);
    }

    /* WHERE 조건들. 객체 배열 + 결합자 배열(where_links) 로 표현. */
    if (sql->where_count > 0) {
        fprintf(out, ",\"where\":[");
        for (int i = 0; i < sql->where_count; i++) {
            if (i) fputc(',', out);
            fputc('{', out);
            fprintf(out, "\"column\":"); emit_str(out, sql->where[i].column);
            fprintf(out, ",\"op\":");    emit_str(out, sql->where[i].op);
            fprintf(out, ",\"value\":"); emit_str(out, sql->where[i].value);
            fputc('}', out);
        }
        fputc(']', out);
        if (sql->where_count > 1) {
            fprintf(out, ",\"where_links\":[");
            for (int i = 1; i < sql->where_count; i++) {
                const char *link = where_link_at(sql, i);
                if (i > 1) fputc(',', out);
                emit_str(out, link ? link : "");
            }
            fputc(']', out);
        }
        /* AND/OR 결합이 있으면 함께. */
        if (sql->where_count > 1 && sql->where_links != NULL) {
            fprintf(out, ",\"where_links\":");
            emit_str_array(out, sql->where_links, sql->where_count - 1);
        } else if (sql->where_logic[0]) {
            fprintf(out, ",\"where_logic\":");
            emit_str(out, sql->where_logic);
        }
    }

    /* UPDATE SET 의 col=val 쌍들 */
    if (sql->set_count > 0) {
        fprintf(out, ",\"set\":[");
        for (int i = 0; i < sql->set_count; i++) {
            if (i) fputc(',', out);
            fputc('{', out);
            fprintf(out, "\"column\":"); emit_str(out, sql->set[i].column);
            fprintf(out, ",\"value\":"); emit_str(out, sql->set[i].value);
            fputc('}', out);
        }
        fputc(']', out);
    }

    /* ORDER BY (있으면). asc 는 true/false 의 boolean 으로 표현. */
    if (sql->order_by) {
        fprintf(out, ",\"order_by\":{\"column\":");
        emit_str(out, sql->order_by->column);
        fprintf(out, ",\"asc\":%s}", sql->order_by->asc ? "true" : "false");
    }

    /* LIMIT (있을 때만) */
    if (sql->limit >= 0) {
        fprintf(out, ",\"limit\":%d", sql->limit);
    }

    fputc('}', out);
    fputc('\n', out);   /* 다음 statement 와 한 줄 띄우기 */
}
