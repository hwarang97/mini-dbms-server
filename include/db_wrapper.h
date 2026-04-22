#ifndef DB_WRAPPER_H
#define DB_WRAPPER_H

typedef struct db_result db_result_t;

int execute_query_safe(const char *sql, db_result_t **out);
void db_result_free(db_result_t *result);

#endif /* DB_WRAPPER_H */
