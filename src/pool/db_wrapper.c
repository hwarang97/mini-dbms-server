#define _POSIX_C_SOURCE 200809L

#include "db_wrapper.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "cJSON.h"
#include "types.h"

static pthread_mutex_t g_db_mutex = PTHREAD_MUTEX_INITIALIZER;

static void free_string_array(char **items, int count)
{
    int index;

    if (items == NULL) {
        return;
    }

    for (index = 0; index < count; ++index) {
        free(items[index]);
    }

    free(items);
}

static char *duplicate_range(const char *start, size_t length)
{
    char *copy = (char *)malloc(length + 1U);

    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, start, length);
    copy[length] = '\0';
    return copy;
}

static char *trim_in_place(char *text)
{
    char *start = text;
    char *end;

    while (*start != '\0' && isspace((unsigned char)*start)) {
        ++start;
    }

    end = start + strlen(start);
    while (end > start && isspace((unsigned char)end[-1])) {
        --end;
    }

    *end = '\0';
    return start;
}

static const char *find_keyword_case_insensitive(const char *text, const char *keyword)
{
    size_t keyword_length = strlen(keyword);
    const char *cursor = text;

    while (*cursor != '\0') {
        if ((cursor == text || isspace((unsigned char)cursor[-1])) &&
            strncasecmp(cursor, keyword, keyword_length) == 0 &&
            (cursor[keyword_length] == '\0' ||
             isspace((unsigned char)cursor[keyword_length]) ||
             cursor[keyword_length] == '(')) {
            return cursor;
        }

        ++cursor;
    }

    return NULL;
}

static int load_schema_columns(const char *table, char ***out_columns, int *out_count)
{
    char schema_path[256];
    FILE *schema_file = NULL;
    char line[256];
    char **columns = NULL;
    int count = 0;
    int capacity = 0;
    int written;

    if (table == NULL || out_columns == NULL || out_count == NULL) {
        return -1;
    }

    written = snprintf(schema_path, sizeof(schema_path), "data/schema/%s.schema", table);
    if (written < 0 || (size_t)written >= sizeof(schema_path)) {
        return -1;
    }

    schema_file = fopen(schema_path, "r");
    if (schema_file == NULL) {
        return -1;
    }

    while (fgets(line, sizeof(line), schema_file) != NULL) {
        char *comma = strchr(line, ',');
        char *column_name;
        char **grown_columns;

        if (comma == NULL) {
            free_string_array(columns, count);
            fclose(schema_file);
            return -1;
        }

        *comma = '\0';
        column_name = trim_in_place(line);
        if (*column_name == '\0') {
            continue;
        }

        if (count == capacity) {
            int new_capacity = (capacity == 0) ? 4 : capacity * 2;

            grown_columns = (char **)realloc(columns,
                                             (size_t)new_capacity * sizeof(*columns));
            if (grown_columns == NULL) {
                free_string_array(columns, count);
                fclose(schema_file);
                return -1;
            }

            columns = grown_columns;
            capacity = new_capacity;
        }

        columns[count] = duplicate_range(column_name, strlen(column_name));
        if (columns[count] == NULL) {
            free_string_array(columns, count);
            fclose(schema_file);
            return -1;
        }

        ++count;
    }

    fclose(schema_file);
    *out_columns = columns;
    *out_count = count;
    return 0;
}

static int build_insert_with_schema_columns(const char *original_sql,
                                           const char *table,
                                           char **out_rewritten_sql)
{
    const char *values_keyword;
    char **columns = NULL;
    int column_count = 0;
    size_t rewritten_length;
    size_t offset = 0;
    int index;
    char *rewritten_sql;

    if (original_sql == NULL || table == NULL || out_rewritten_sql == NULL) {
        return -1;
    }

    values_keyword = find_keyword_case_insensitive(original_sql, "VALUES");
    if (values_keyword == NULL) {
        return -1;
    }

    if (load_schema_columns(table, &columns, &column_count) != 0 || column_count == 0) {
        free_string_array(columns, column_count);
        return -1;
    }

    rewritten_length = strlen("INSERT INTO ") + strlen(table) + strlen(" () ") +
                       strlen(values_keyword);
    for (index = 0; index < column_count; ++index) {
        rewritten_length += strlen(columns[index]);
        if (index + 1 < column_count) {
            rewritten_length += 2U;
        }
    }

    rewritten_sql = (char *)malloc(rewritten_length + 1U);
    if (rewritten_sql == NULL) {
        free_string_array(columns, column_count);
        return -1;
    }

    offset += (size_t)snprintf(rewritten_sql + offset, rewritten_length + 1U - offset,
                               "INSERT INTO %s (", table);
    for (index = 0; index < column_count; ++index) {
        offset += (size_t)snprintf(rewritten_sql + offset, rewritten_length + 1U - offset,
                                   "%s%s", columns[index],
                                   (index + 1 < column_count) ? ", " : "");
    }
    (void)snprintf(rewritten_sql + offset, rewritten_length + 1U - offset,
                   ") %s", values_keyword);

    free_string_array(columns, column_count);
    *out_rewritten_sql = rewritten_sql;
    return 0;
}

static ParsedSQL *parse_sql_with_insert_fallback(const char *sql, char **rewritten_sql)
{
    ParsedSQL *parsed = parse_sql(sql);

    if (parsed == NULL) {
        return NULL;
    }

    if (parsed->type == QUERY_INSERT &&
        (parsed->columns == NULL || parsed->col_count == 0 ||
         parsed->values == NULL || parsed->val_count == 0) &&
        build_insert_with_schema_columns(sql, parsed->table, rewritten_sql) == 0) {
        free_parsed(parsed);
        parsed = parse_sql(*rewritten_sql);
    }

    return parsed;
}

static int build_simple_ok_json(char **out_json)
{
    cJSON *root = cJSON_CreateObject();

    if (root == NULL) {
        return -1;
    }

    if (cJSON_AddStringToObject(root, "status", "ok") == NULL) {
        cJSON_Delete(root);
        return -1;
    }

    *out_json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return (*out_json != NULL) ? 0 : -1;
}

static int build_select_json(const RowSet *rowset, char **out_json)
{
    cJSON *root = NULL;
    cJSON *rows = NULL;
    int row_index;

    if (rowset == NULL || out_json == NULL) {
        return -1;
    }

    root = cJSON_CreateObject();
    if (root == NULL) {
        return -1;
    }

    if (cJSON_AddStringToObject(root, "status", "ok") == NULL ||
        cJSON_AddNumberToObject(root, "count", rowset->row_count) == NULL) {
        cJSON_Delete(root);
        return -1;
    }

    rows = cJSON_AddArrayToObject(root, "rows");
    if (rows == NULL) {
        cJSON_Delete(root);
        return -1;
    }

    for (row_index = 0; row_index < rowset->row_count; ++row_index) {
        cJSON *row_object = cJSON_CreateObject();
        int column_index;

        if (row_object == NULL) {
            cJSON_Delete(root);
            return -1;
        }

        for (column_index = 0; column_index < rowset->col_count; ++column_index) {
            const char *column_name = rowset->col_names[column_index];
            const char *value = rowset->rows[row_index][column_index];

            if (cJSON_AddStringToObject(row_object,
                                        (column_name != NULL) ? column_name : "",
                                        (value != NULL) ? value : "") == NULL) {
                cJSON_Delete(row_object);
                cJSON_Delete(root);
                return -1;
            }
        }

        cJSON_AddItemToArray(rows, row_object);
    }

    *out_json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return (*out_json != NULL) ? 0 : -1;
}

static int execute_parsed_sql(ParsedSQL *parsed_sql, char **out_json)
{
    RowSet *rowset = NULL;
    int status = -1;

    switch (parsed_sql->type) {
    case QUERY_SELECT:
        if (storage_select_result(parsed_sql->table, parsed_sql, &rowset) == 0) {
            status = build_select_json(rowset, out_json);
        }
        break;

    case QUERY_CREATE:
        if (storage_create(parsed_sql->table, parsed_sql->col_defs,
                           parsed_sql->col_def_count) == 0) {
            status = build_simple_ok_json(out_json);
        }
        break;

    case QUERY_INSERT:
        if (storage_insert(parsed_sql->table, parsed_sql->columns,
                           parsed_sql->values, parsed_sql->val_count) == 0) {
            status = build_simple_ok_json(out_json);
        }
        break;

    case QUERY_DELETE:
        if (storage_delete(parsed_sql->table, parsed_sql) == 0) {
            status = build_simple_ok_json(out_json);
        }
        break;

    case QUERY_UPDATE:
        if (storage_update(parsed_sql->table, parsed_sql) == 0) {
            status = build_simple_ok_json(out_json);
        }
        break;

    case QUERY_UNKNOWN:
    default:
        status = -1;
        break;
    }

    rowset_free(rowset);
    return status;
}

int execute_query_safe(const char *sql, char **out_json)
{
    ParsedSQL *parsed_sql = NULL;
    char *rewritten_sql = NULL;
    int status = -1;

    if (sql == NULL || out_json == NULL) {
        return -1;
    }

    *out_json = NULL;

    pthread_mutex_lock(&g_db_mutex);

    parsed_sql = parse_sql_with_insert_fallback(sql, &rewritten_sql);
    if (parsed_sql == NULL || parsed_sql->type == QUERY_UNKNOWN) {
        goto cleanup;
    }

    status = execute_parsed_sql(parsed_sql, out_json);

cleanup:
    free_parsed(parsed_sql);
    free(rewritten_sql);
    pthread_mutex_unlock(&g_db_mutex);

    if (status != 0) {
        free(*out_json);
        *out_json = NULL;
    }

    return status;
}
