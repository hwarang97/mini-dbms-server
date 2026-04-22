#include "http.h"

#include <dirent.h>
#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "cJSON.h"
#include "common.h"
#include "job_queue.h"

#define HEADER_BUFFER_LIMIT  (16U * 1024U)
#define BODY_BUFFER_LIMIT    (1024U * 1024U)
#define REQUEST_BUFFER_LIMIT (HEADER_BUFFER_LIMIT + BODY_BUFFER_LIMIT)
#define READ_CHUNK_SIZE      4096U

enum {
    HTTP_BAD_REQUEST = 400,
    HTTP_NOT_FOUND = 404,
    HTTP_METHOD_NOT_ALLOWED = 405,
    HTTP_PAYLOAD_TOO_LARGE = 413,
    HTTP_INTERNAL_SERVER_ERROR = 500
};

typedef struct {
    char *buffer;
    size_t header_end;
    size_t body_length;
} raw_http_request_t;

typedef struct {
    char *method;
    char *path;
    char *version;
    char *body;
    size_t body_length;
} parsed_http_request_t;

static int write_all(int fd, const char *buffer, size_t length)
{
    size_t written = 0;

    while (written < length) {
        ssize_t rc = write(fd, buffer + written, length - written);

        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (rc == 0) {
            return -1;
        }

        written += (size_t)rc;
    }

    return 0;
}

static const char *reason_phrase_for_status(int status_code)
{
    switch (status_code) {
    case HTTP_NOT_FOUND:
        return "Not Found";
    case HTTP_METHOD_NOT_ALLOWED:
        return "Method Not Allowed";
    case HTTP_PAYLOAD_TOO_LARGE:
        return "Payload Too Large";
    case HTTP_INTERNAL_SERVER_ERROR:
        return "Internal Server Error";
    case HTTP_BAD_REQUEST:
    default:
        return "Bad Request";
    }
}

static const char *error_body_for_status(int status_code)
{
    switch (status_code) {
    case HTTP_NOT_FOUND:
        return "{\"error\":\"not found\"}";
    case HTTP_METHOD_NOT_ALLOWED:
        return "{\"error\":\"method not allowed\"}";
    case HTTP_PAYLOAD_TOO_LARGE:
        return "{\"error\":\"payload too large\"}";
    case HTTP_INTERNAL_SERVER_ERROR:
        return "{\"error\":\"internal server error\"}";
    case HTTP_BAD_REQUEST:
    default:
        return "{\"error\":\"bad request\"}";
    }
}

static void send_response(int client_fd, int status_code, const char *reason_phrase, const char *body)
{
    char header[256];
    size_t body_length = strlen(body);
    int header_length = snprintf(header, sizeof(header),
                                 "HTTP/1.1 %d %s\r\n"
                                 "Content-Type: application/json\r\n"
                                 "Content-Length: %zu\r\n"
                                 "Connection: close\r\n"
                                 "\r\n",
                                 status_code, reason_phrase, body_length);

    if (header_length < 0 || (size_t)header_length >= sizeof(header)) {
        return;
    }

    (void)write_all(client_fd, header, (size_t)header_length);
    (void)write_all(client_fd, body, body_length);
}

static void send_error_and_close(int client_fd, int status_code)
{
    send_response(client_fd, status_code, reason_phrase_for_status(status_code),
                  error_body_for_status(status_code));
    (void)close(client_fd);
}

static int has_suffix(const char *text, const char *suffix)
{
    size_t text_length = strlen(text);
    size_t suffix_length = strlen(suffix);

    if (text_length < suffix_length) {
        return 0;
    }

    return strcmp(text + text_length - suffix_length, suffix) == 0;
}

static char *build_tables_response_body(void)
{
    DIR *directory = NULL;
    struct dirent *entry;
    cJSON *tables = cJSON_CreateArray();
    char *body = NULL;

    if (tables == NULL) {
        return NULL;
    }

    directory = opendir("data/schema");
    if (directory == NULL) {
        body = cJSON_PrintUnformatted(tables);
        cJSON_Delete(tables);
        return body;
    }

    while ((entry = readdir(directory)) != NULL) {
        size_t table_name_length;
        char *table_name;

        if (entry->d_name[0] == '.' || !has_suffix(entry->d_name, ".schema")) {
            continue;
        }

        table_name_length = strlen(entry->d_name) - strlen(".schema");
        table_name = (char *)malloc(table_name_length + 1U);
        if (table_name == NULL) {
            closedir(directory);
            cJSON_Delete(tables);
            return NULL;
        }

        memcpy(table_name, entry->d_name, table_name_length);
        table_name[table_name_length] = '\0';

        if (cJSON_AddItemToArray(tables, cJSON_CreateString(table_name)) == 0) {
            free(table_name);
            closedir(directory);
            cJSON_Delete(tables);
            return NULL;
        }

        free(table_name);
    }

    closedir(directory);
    body = cJSON_PrintUnformatted(tables);
    cJSON_Delete(tables);
    return body;
}

static size_t find_double_crlf(const char *buffer, size_t length)
{
    size_t i;

    if (length < 4U) {
        return SIZE_MAX;
    }

    for (i = 0; i + 3U < length; ++i) {
        if (buffer[i] == '\r' && buffer[i + 1U] == '\n' &&
            buffer[i + 2U] == '\r' && buffer[i + 3U] == '\n') {
            return i;
        }
    }

    return SIZE_MAX;
}

static int header_name_equals(const char *candidate, size_t candidate_length, const char *expected)
{
    size_t i;
    size_t expected_length = strlen(expected);

    if (candidate_length != expected_length) {
        return 0;
    }

    for (i = 0; i < expected_length; ++i) {
        if (tolower((unsigned char)candidate[i]) != tolower((unsigned char)expected[i])) {
            return 0;
        }
    }

    return 1;
}

static int parse_content_length_value(const char *value, size_t value_length, size_t *out_length)
{
    char number_buffer[32];
    size_t start = 0;
    size_t end = value_length;
    char *parse_end = NULL;
    unsigned long long parsed = 0;

    while (start < value_length &&
           (value[start] == ' ' || value[start] == '\t')) {
        ++start;
    }

    while (end > start &&
           (value[end - 1U] == ' ' || value[end - 1U] == '\t')) {
        --end;
    }

    if (start == end) {
        return HTTP_BAD_REQUEST;
    }

    if (end - start >= sizeof(number_buffer)) {
        return HTTP_BAD_REQUEST;
    }

    memcpy(number_buffer, value + start, end - start);
    number_buffer[end - start] = '\0';

    errno = 0;
    parsed = strtoull(number_buffer, &parse_end, 10);
    if (errno != 0 || parse_end == number_buffer || *parse_end != '\0') {
        return HTTP_BAD_REQUEST;
    }

    if (parsed > BODY_BUFFER_LIMIT || parsed > SIZE_MAX) {
        return HTTP_PAYLOAD_TOO_LARGE;
    }

    *out_length = (size_t)parsed;
    return 0;
}

static int extract_content_length(const char *buffer, size_t header_end, size_t *out_length)
{
    const char *cursor = buffer;
    const char *headers_end = buffer + header_end;
    int saw_content_length = 0;

    while (cursor < headers_end && *cursor != '\n') {
        ++cursor;
    }

    if (cursor >= headers_end) {
        return HTTP_BAD_REQUEST;
    }

    ++cursor;

    while (cursor < headers_end) {
        const char *line_end = cursor;
        const char *colon = NULL;
        size_t name_length;
        int parse_status;
        size_t parsed_length = 0;

        while (line_end < headers_end && *line_end != '\r') {
            ++line_end;
        }

        if (line_end > headers_end || line_end[1] != '\n') {
            return HTTP_BAD_REQUEST;
        }

        if (line_end == cursor) {
            break;
        }

        colon = memchr(cursor, ':', (size_t)(line_end - cursor));
        if (colon == NULL) {
            return HTTP_BAD_REQUEST;
        }

        name_length = (size_t)(colon - cursor);
        if (!header_name_equals(cursor, name_length, "Content-Length")) {
            cursor = line_end + 2;
            continue;
        }

        if (saw_content_length) {
            return HTTP_BAD_REQUEST;
        }

        parse_status = parse_content_length_value(colon + 1,
                                                  (size_t)(line_end - (colon + 1)),
                                                  &parsed_length);
        if (parse_status != 0) {
            return parse_status;
        }

        *out_length = parsed_length;
        saw_content_length = 1;
        cursor = line_end + 2;
    }

    if (!saw_content_length) {
        *out_length = 0;
    }
    return 0;
}

static int grow_request_buffer(char **buffer, size_t *capacity)
{
    char *grown_buffer;
    size_t new_capacity = *capacity * 2U;

    if (new_capacity > REQUEST_BUFFER_LIMIT) {
        new_capacity = REQUEST_BUFFER_LIMIT;
    }

    if (new_capacity <= *capacity) {
        return HTTP_PAYLOAD_TOO_LARGE;
    }

    grown_buffer = (char *)realloc(*buffer, new_capacity + 1U);
    if (grown_buffer == NULL) {
        return HTTP_INTERNAL_SERVER_ERROR;
    }

    *buffer = grown_buffer;
    *capacity = new_capacity;
    return 0;
}

static int update_expected_request_size(const char *buffer, size_t total_read,
                                        size_t *header_end, size_t *body_length,
                                        size_t *expected_total)
{
    int status;

    if (*header_end != SIZE_MAX) {
        return 0;
    }

    if (total_read > HEADER_BUFFER_LIMIT) {
        return HTTP_PAYLOAD_TOO_LARGE;
    }

    *header_end = find_double_crlf(buffer, total_read);
    if (*header_end == SIZE_MAX) {
        return 0;
    }

    status = extract_content_length(buffer, *header_end, body_length);
    if (status != 0) {
        return status;
    }

    *expected_total = *header_end + 4U + *body_length;
    if (*expected_total > REQUEST_BUFFER_LIMIT) {
        return HTTP_PAYLOAD_TOO_LARGE;
    }

    return 0;
}

static int read_http_request(int client_fd, raw_http_request_t *request)
{
    char *buffer = NULL;
    size_t capacity = READ_CHUNK_SIZE;
    size_t total_read = 0;
    size_t header_end = SIZE_MAX;
    size_t body_length = 0;
    size_t expected_total = 0;

    buffer = (char *)malloc(capacity + 1U);
    if (buffer == NULL) {
        return HTTP_INTERNAL_SERVER_ERROR;
    }

    for (;;) {
        ssize_t bytes_read;
        int status;

        if (total_read == capacity) {
            status = grow_request_buffer(&buffer, &capacity);
            if (status != 0) {
                free(buffer);
                return status;
            }
        }

        bytes_read = read(client_fd, buffer + total_read, capacity - total_read);
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }

            free(buffer);
            return HTTP_INTERNAL_SERVER_ERROR;
        }

        if (bytes_read == 0) {
            break;
        }

        total_read += (size_t)bytes_read;
        buffer[total_read] = '\0';

        status = update_expected_request_size(buffer, total_read, &header_end,
                                              &body_length, &expected_total);
        if (status != 0) {
            free(buffer);
            return status;
        }

        if (header_end != SIZE_MAX && total_read >= expected_total) {
            break;
        }
    }

    if (header_end == SIZE_MAX || total_read < expected_total) {
        free(buffer);
        return HTTP_BAD_REQUEST;
    }

    buffer[expected_total] = '\0';
    request->buffer = buffer;
    request->header_end = header_end;
    request->body_length = body_length;
    return 0;
}

static int split_request_line(char *request_line, char **method, char **path, char **version)
{
    char *space1 = NULL;
    char *space2 = NULL;

    space1 = strchr(request_line, ' ');
    if (space1 == NULL || space1 == request_line) {
        return HTTP_BAD_REQUEST;
    }

    *space1 = '\0';
    space2 = strchr(space1 + 1, ' ');
    if (space2 == NULL || space2 == space1 + 1) {
        return HTTP_BAD_REQUEST;
    }

    *space2 = '\0';
    if (strchr(space2 + 1, ' ') != NULL || *(space2 + 1) == '\0') {
        return HTTP_BAD_REQUEST;
    }

    *method = request_line;
    *path = space1 + 1;
    *version = space2 + 1;
    return 0;
}

static int validate_http_version(const char *version)
{
    if (strcmp(version, "HTTP/1.1") != 0 && strcmp(version, "HTTP/1.0") != 0) {
        return HTTP_BAD_REQUEST;
    }

    return 0;
}

static int parse_http_request(raw_http_request_t *raw_request, parsed_http_request_t *parsed_request)
{
    char *request_line_end = strstr(raw_request->buffer, "\r\n");
    int status;

    if (request_line_end == NULL) {
        return HTTP_BAD_REQUEST;
    }

    raw_request->buffer[raw_request->header_end] = '\0';
    *request_line_end = '\0';

    status = split_request_line(raw_request->buffer, &parsed_request->method,
                                &parsed_request->path, &parsed_request->version);
    if (status != 0) {
        return status;
    }

    status = validate_http_version(parsed_request->version);
    if (status != 0) {
        return status;
    }

    parsed_request->body = raw_request->buffer + raw_request->header_end + 4U;
    parsed_request->body_length = raw_request->body_length;
    return 0;
}

/* caller frees unless ownership moves into a queued job */
static int extract_sql_from_body(const parsed_http_request_t *request, char **sql_copy)
{
    cJSON *payload = NULL;
    cJSON *sql_item = NULL;
    size_t sql_length;

    payload = cJSON_ParseWithLength(request->body, request->body_length);
    if (payload == NULL) {
        return HTTP_BAD_REQUEST;
    }

    sql_item = cJSON_GetObjectItemCaseSensitive(payload, "sql");
    if (!cJSON_IsString(sql_item) || sql_item->valuestring == NULL) {
        cJSON_Delete(payload);
        return HTTP_BAD_REQUEST;
    }

    sql_length = strlen(sql_item->valuestring);
    if (sql_length == 0U) {
        cJSON_Delete(payload);
        return HTTP_BAD_REQUEST;
    }

    *sql_copy = (char *)malloc(sql_length + 1U);
    if (*sql_copy == NULL) {
        cJSON_Delete(payload);
        return HTTP_INTERNAL_SERVER_ERROR;
    }

    memcpy(*sql_copy, sql_item->valuestring, sql_length + 1U);
    cJSON_Delete(payload);
    return 0;
}

static int handle_non_query_request(int client_fd, const parsed_http_request_t *request)
{
    if (strcmp(request->method, "GET") == 0 && strcmp(request->path, "/health") == 0) {
        send_response(client_fd, 200, "OK", "{\"status\":\"ok\"}");
        (void)close(client_fd);
        return 1;
    }

    if (strcmp(request->method, "GET") == 0 && strcmp(request->path, "/tables") == 0) {
        char *body = build_tables_response_body();

        if (body == NULL) {
            send_error_and_close(client_fd, HTTP_INTERNAL_SERVER_ERROR);
            return 1;
        }

        send_response(client_fd, 200, "OK", body);
        free(body);
        (void)close(client_fd);
        return 1;
    }

    if (strcmp(request->path, "/query") == 0 && strcmp(request->method, "POST") != 0) {
        send_error_and_close(client_fd, HTTP_METHOD_NOT_ALLOWED);
        return 1;
    }

    if (strcmp(request->method, "POST") != 0 || strcmp(request->path, "/query") != 0) {
        send_error_and_close(client_fd, HTTP_NOT_FOUND);
        return 1;
    }

    return 0;
}

/* worker frees after processing once queue_push succeeds */
static job_t *create_job(int client_fd, char *sql_copy)
{
    job_t *job = NULL;

    /* worker frees after processing */
    job = (job_t *)malloc(sizeof(*job));
    if (job == NULL) {
        return NULL;
    }

    job->client_fd = client_fd;
    job->sql = sql_copy; /* worker frees after processing */
    return job;
}

static void destroy_job(job_t *job)
{
    if (job == NULL) {
        return;
    }

    free(job->sql);
    free(job);
}

static int submit_job(job_t *job)
{
    if (g_queue == NULL || queue_push(g_queue, job) != 0) {
        return HTTP_INTERNAL_SERVER_ERROR;
    }

    return 0;
}

void handle_connection(int client_fd)
{
    raw_http_request_t raw_request = { 0 };
    parsed_http_request_t parsed_request = { 0 };
    char *sql_copy = NULL;
    job_t *job = NULL;
    int status = read_http_request(client_fd, &raw_request);

    if (status != 0) {
        send_error_and_close(client_fd, status);
        return;
    }

    status = parse_http_request(&raw_request, &parsed_request);
    if (status != 0) {
        goto cleanup;
    }

    if (handle_non_query_request(client_fd, &parsed_request)) {
        free(raw_request.buffer);
        return;
    }

    status = extract_sql_from_body(&parsed_request, &sql_copy);
    if (status != 0) {
        goto cleanup;
    }

    job = create_job(client_fd, sql_copy);
    if (job == NULL) {
        status = HTTP_INTERNAL_SERVER_ERROR;
        goto cleanup;
    }
    sql_copy = NULL;

    status = submit_job(job);
    if (status != 0) {
        goto cleanup;
    }

    free(raw_request.buffer);
    return;

cleanup:
    destroy_job(job);
    free(sql_copy);
    free(raw_request.buffer);
    send_error_and_close(client_fd, status);
}
