#ifndef DB_WRAPPER_H
#define DB_WRAPPER_H

/* Execute sql and store a malloc'd JSON string into *out_json.
 * Returns 0 on success, -1 on failure.
 * caller frees *out_json */
int execute_query_safe(const char *sql, char **out_json);

#endif /* DB_WRAPPER_H */
