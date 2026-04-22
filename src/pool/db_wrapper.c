#include "db_wrapper.h"

#include <stdlib.h>

struct db_result {
    int placeholder;
};

int execute_query_safe(const char *sql, db_result_t **out)
{
    (void)sql;
    if (out != NULL) {
        *out = NULL;
    }
    return -1;
}

void db_result_free(db_result_t *result)
{
    free(result);
}
