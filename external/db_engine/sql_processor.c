#define _POSIX_C_SOURCE 200809L

#include "sql_processor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int sql_processor_status(void)
{
    return 0;
}

static char *read_file(const char *path)
{
    FILE *fp;
    long size;
    char *buffer;

    fp = fopen(path, "rb");
    if (!fp) {
        perror(path);
        return NULL;
    }

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }

    size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        return NULL;
    }

    if (fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }

    buffer = (char *)malloc((size_t)size + 1);
    if (!buffer) {
        fclose(fp);
        return NULL;
    }

    if (fread(buffer, 1, (size_t)size, fp) != (size_t)size) {
        free(buffer);
        fclose(fp);
        return NULL;
    }

    buffer[size] = '\0';
    fclose(fp);
    return buffer;
}

static void process_statement(const char *statement, SQLProcessorOptions options)
{
    ParsedSQL *sql;

    if (options.tokens_mode) {
        print_tokens(stdout, statement);
        return;
    }

    sql = parse_sql(statement);
    if (sql && sql->type != QUERY_UNKNOWN) {
        if (options.debug_mode) {
            print_ast(stdout, sql);
        }
        if (options.json_mode) {
            print_json(stdout, sql);
        }
        if (options.format_mode) {
            print_format(stdout, sql);
        }
        execute(sql);
    }

    free_parsed(sql);
}

int sql_processor_run_string(const char *input, SQLProcessorOptions options)
{
    const char *cursor;
    const char *start;
    char quote;

    if (!input) {
        return -1;
    }

    cursor = input;
    start = input;
    quote = 0;

    while (*cursor) {
        if (quote) {
            if (*cursor == quote) {
                quote = 0;
            }
        } else if (*cursor == '\'' || *cursor == '"') {
            quote = *cursor;
        } else if (*cursor == ';') {
            size_t length = (size_t)(cursor - start);
            char *statement = (char *)malloc(length + 1);
            if (!statement) {
                return -1;
            }

            memcpy(statement, start, length);
            statement[length] = '\0';
            process_statement(statement, options);
            free(statement);
            start = cursor + 1;
        }
        cursor++;
    }

    while (*start == ' ' || *start == '\n' || *start == '\t' || *start == '\r') {
        start++;
    }

    if (*start) {
        process_statement(start, options);
    }

    return 0;
}

int sql_processor_run_file(const char *path, SQLProcessorOptions options)
{
    char *input = read_file(path);
    int status;

    if (!input) {
        return -1;
    }

    status = sql_processor_run_string(input, options);
    free(input);
    return status;
}
