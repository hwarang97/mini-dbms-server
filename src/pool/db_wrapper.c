#define _POSIX_C_SOURCE 200809L

#include "db_wrapper.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * execute_query_safe — SQL 문자열을 받아서 실행하고 결과를 JSON 문자열로 반환한다.
 *
 * 현재는 스텁(stub) 구현으로, 실제 DB 엔진 호출 대신
 * 입력받은 SQL을 그대로 JSON에 담아서 반환한다.
 * 통합 단계에서 external/db_engine의 실제 execute()로 교체 예정.
 *
 * 파라미터:
 *   sql      — 실행할 SQL 문자열 (worker가 넘겨줌)
 *   out_json — 결과 JSON 문자열을 저장할 포인터. caller frees
 *
 * 반환값: 성공 0, 실패 -1
 */
int execute_query_safe(const char *sql, char **out_json)
{
    char *json;
    size_t sql_len;
    size_t json_len;
    size_t i;
    size_t j;
    char *escaped;

    if (sql == NULL || out_json == NULL) {
        return -1;
    }

    sql_len = strlen(sql);

    /*
     * JSON 문자열 안에 큰따옴표(")나 백슬래시(\)가 있으면
     * 앞에 \를 붙여야 올바른 JSON이 된다.
     * 최악의 경우 모든 글자가 이스케이프되므로 sql_len * 2 + 1 만큼 확보.
     */
    escaped = (char *)malloc(sql_len * 2 + 1); /* caller(this func) frees */
    if (escaped == NULL) {
        return -1;
    }

    for (i = 0, j = 0; i < sql_len; i++) {
        if (sql[i] == '"' || sql[i] == '\\') {
            escaped[j++] = '\\'; /* 이스케이프 문자 삽입 */
        }
        escaped[j++] = sql[i];
    }
    escaped[j] = '\0';

    /* {"status":"ok","query":"<escaped_sql>"} 형태로 JSON 문자열 생성 */
    json_len = j + 32; /* 고정 키/값 길이 여유분 포함 */
    json = (char *)malloc(json_len + strlen("{\"status\":\"ok\",\"query\":\"\"}"));
    if (json == NULL) {
        free(escaped);
        return -1;
    }

    snprintf(json, json_len + 32, "{\"status\":\"ok\",\"query\":\"%s\"}", escaped);
    free(escaped);

    *out_json = json; /* caller frees */
    return 0;
}
