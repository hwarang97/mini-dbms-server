/* executor.c — 파싱된 쿼리를 storage 로 보내는 우체부 (스텁)
 * ============================================================================
 *
 * ▣ 이 파일이 하는 일
 *   파서가 만든 ParsedSQL 을 받아서 "이건 SELECT 니까 storage_select 에 보내고,
 *   저건 INSERT 니까 storage_insert 에 보낸다" 식으로 적절한 함수를 호출한다.
 *
 * ▣ 왜 따로 두지?
 *   파서는 "글자 → 구조체" 만 책임지고, storage 는 "데이터 저장/읽기" 만
 *   책임진다. 그 사이에서 "구조체 보고 어떤 storage 함수 부를지" 를 결정하는
 *   사람이 따로 필요한데, 그게 executor.
 *
 * ▣ 현재 상태
 *   디스패치 (분기) 만 작성된 스텁. case 본문의 실제 동작은
 *   - SELECT       → 석제
 *   - INSERT/UPDATE/DELETE → 원우 또는 세인 (경쟁)
 *   가 채울 예정.
 *
 *   CREATE 분기는 지용이 1차로 작성한 형태가 들어있다.
 * ============================================================================
 */

#include "types.h"
#include <stdio.h>

/* execute: ParsedSQL 을 받아서 종류에 맞는 storage_* 함수를 호출. */
void execute(ParsedSQL *sql) {
    if (!sql) return;

    switch (sql->type) {
        case QUERY_CREATE:
            /* 테이블 만들기: 컬럼 정의들을 storage 로 넘긴다. */
            storage_create(sql->table, sql->col_defs, sql->col_def_count);
            break;

        case QUERY_INSERT:
            /* 새 행 추가: 컬럼명 배열과 값 배열을 storage 로. */
            storage_insert(sql->table, sql->columns, sql->values, sql->val_count);
            break;

        case QUERY_SELECT:
            /* 조회: WHERE / ORDER BY / LIMIT 정보가 sql 안에 다 있어서
             * sql 자체를 그대로 넘겨준다. */
            storage_select(sql->table, sql);
            break;

        case QUERY_DELETE:
            /* 삭제: WHERE 조건만 storage 에. */
            storage_delete(sql->table, sql);
            break;

        case QUERY_UPDATE:
            /* 수정: SET 와 WHERE 둘 다 필요. */
            storage_update(sql->table, sql);
            break;

        default:
            fprintf(stderr, "[executor] unknown query type\n");
            break;
    }
}
