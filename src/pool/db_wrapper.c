/*
 * db_wrapper.c — DB 엔진 래퍼
 *
 * 이 파일이 하는 일:
 *   SQL 문자열을 받아서 DB 엔진에 실행하고, 결과를 JSON 문자열로 반환한다.
 *
 * 현재 상태 (스텁):
 *   실제 DB 엔진(external/db_engine) 연결은 통합 단계에서 팀 전체가 함께 한다.
 *   지금은 받은 SQL을 그대로 JSON에 담아서 반환하는 스텁으로 구현되어 있다.
 *
 *   예) sql = "SELECT * FROM users"
 *       반환 → {"status":"ok","query":"SELECT * FROM users"}
 *
 * 통합 단계에서 교체할 부분:
 *   execute_query_safe() 내부에서 sql_processor_run_string() 또는
 *   execute() 를 호출해 실제 결과를 JSON으로 변환하면 된다.
 */

#define _POSIX_C_SOURCE 200809L

#include "db_wrapper.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * execute_query_safe — SQL을 실행하고 결과를 JSON 문자열로 반환한다.
 *
 * 파라미터:
 *   sql      : 실행할 SQL 문자열. process_job이 넘겨줌.
 *   out_json : 결과 JSON 문자열을 저장할 포인터.
 *              이 함수 안에서 malloc하고, caller(process_job)가 free 책임.
 *
 * 반환값: 성공 0, 실패 -1
 *
 * 현재 동작 (스텁):
 *   SQL을 실제로 실행하지 않고, SQL 문자열을 JSON에 담아서 그대로 반환한다.
 *   JSON 형태: {"status":"ok","query":"<sql>"}
 *
 *   SQL 안에 큰따옴표(")나 백슬래시(\)가 있으면 JSON 규칙상 앞에 \를 붙여야 한다.
 *   예) sql = 'SELECT "name"'
 *       JSON → {"status":"ok","query":"SELECT \"name\""}
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
     * 이스케이프 처리를 위한 임시 버퍼 할당.
     *
     * JSON 문자열 안에서 " 와 \ 는 특수문자라서 앞에 \ 를 붙여야 한다.
     * 최악의 경우 모든 글자가 이스케이프되므로 sql_len * 2 + 1 만큼 확보.
     * (예: "aaa" → \"aaa\" 로 길이가 2배가 될 수 있음)
     *
     * 이 버퍼는 이 함수 안에서만 사용하고 아래에서 해제한다.
     */
    escaped = (char *)malloc(sql_len * 2 + 1); /* this func frees */
    if (escaped == NULL) {
        return -1;
    }

    /* SQL을 한 글자씩 읽으면서 " 와 \ 앞에 \ 삽입 */
    for (i = 0, j = 0; i < sql_len; i++) {
        if (sql[i] == '"' || sql[i] == '\\') {
            escaped[j++] = '\\'; /* 이스케이프 문자 삽입 */
        }
        escaped[j++] = sql[i];  /* 원래 글자 복사 */
    }
    escaped[j] = '\0'; /* 문자열 끝 표시 */

    /*
     * 최종 JSON 문자열 버퍼 할당.
     *
     * {"status":"ok","query":"<escaped_sql>"} 형태로 만든다.
     * j = 이스케이프된 sql 길이, 32 = 나머지 고정 문자열 길이 여유분.
     *
     * caller(process_job)가 free 책임.
     */
    json_len = j + 32;
    json = (char *)malloc(json_len + strlen("{\"status\":\"ok\",\"query\":\"\"}"));
    if (json == NULL) {
        free(escaped);
        return -1;
    }

    /* JSON 문자열 완성 */
    snprintf(json, json_len + 32, "{\"status\":\"ok\",\"query\":\"%s\"}", escaped);

    free(escaped); /* 임시 이스케이프 버퍼 해제 */

    *out_json = json; /* caller frees */
    return 0;
}
