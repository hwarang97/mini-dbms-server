#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"
#include "http.h"
#include "job_queue.h"

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

        written += (size_t)rc;
    }

    return 0;
}

static char *read_all(int fd)
{
    size_t capacity = 256;
    size_t length = 0;
    char *buffer = (char *)malloc(capacity);

    assert(buffer != NULL);

    for (;;) {
        ssize_t rc;

        if (length + 1U == capacity) {
            char *grown = (char *)realloc(buffer, capacity * 2U);

            assert(grown != NULL);
            buffer = grown;
            capacity *= 2U;
        }

        rc = read(fd, buffer + length, capacity - length - 1U);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }

            free(buffer);
            return NULL;
        }

        if (rc == 0) {
            break;
        }

        length += (size_t)rc;
    }

    buffer[length] = '\0';
    return buffer;
}

static char *build_request(const char *method, const char *path, const char *body)
{
    const char *body_text = (body != NULL) ? body : "";
    size_t body_length = strlen(body_text);
    int written;
    char *request;

    written = snprintf(NULL, 0,
                       "%s %s HTTP/1.1\r\n"
                       "Host: localhost\r\n"
                       "Content-Type: application/json\r\n"
                       "Content-Length: %zu\r\n"
                       "\r\n"
                       "%s",
                       method, path, body_length, body_text);
    assert(written >= 0);

    request = (char *)malloc((size_t)written + 1U);
    assert(request != NULL);

    written = snprintf(request, (size_t)written + 1U,
                       "%s %s HTTP/1.1\r\n"
                       "Host: localhost\r\n"
                       "Content-Type: application/json\r\n"
                       "Content-Length: %zu\r\n"
                       "\r\n"
                       "%s",
                       method, path, body_length, body_text);
    assert(written >= 0);

    return request;
}

static char *run_request_and_collect_response(const char *request)
{
    int sockets[2];
    char *response;

    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    assert(write_all(sockets[1], request, strlen(request)) == 0);
    assert(shutdown(sockets[1], SHUT_WR) == 0);

    handle_connection(sockets[0]);
    response = read_all(sockets[1]);

    close(sockets[1]);
    return response;
}

static job_t *submit_query_and_pop_job(const char *request, int *out_peer_fd)
{
    int sockets[2];
    job_t *job;

    assert(g_queue != NULL);
    assert(out_peer_fd != NULL);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    assert(write_all(sockets[1], request, strlen(request)) == 0);
    assert(shutdown(sockets[1], SHUT_WR) == 0);

    handle_connection(sockets[0]);
    job = queue_pop(g_queue);

    assert(job != NULL);
    *out_peer_fd = sockets[1];
    return job;
}

static void cleanup_queued_job(job_t *job, int peer_fd)
{
    assert(job != NULL);
    close(job->client_fd);
    close(peer_fd);
    free(job->sql);
    free(job);
}

static void with_queue(job_queue_t **out_queue)
{
    job_queue_t *queue = queue_init(4);

    assert(queue != NULL);
    g_queue = queue;
    *out_queue = queue;
}

static void cleanup_queue(job_queue_t *queue)
{
    g_queue = NULL;
    queue_destroy(queue);
}

static void create_file_with_contents(const char *path, const char *contents)
{
    FILE *fp = fopen(path, "w");

    assert(fp != NULL);
    assert(fputs(contents, fp) >= 0);
    assert(fclose(fp) == 0);
}

static void test_post_query_enqueues_sql_job(void)
{
    job_queue_t *queue;
    char *request = build_request("POST", "/query", "{\"sql\":\"SELECT * FROM users\"}");
    int peer_fd = -1;
    job_t *job;

    with_queue(&queue);
    job = submit_query_and_pop_job(request, &peer_fd);

    assert(strcmp(job->sql, "SELECT * FROM users") == 0);
    assert(job->client_fd >= 0);

    cleanup_queued_job(job, peer_fd);
    cleanup_queue(queue);
    free(request);
}

static void test_get_health_returns_ok(void)
{
    char *request = build_request("GET", "/health", "");
    char *response = run_request_and_collect_response(request);

    assert(response != NULL);
    assert(strstr(response, "HTTP/1.1 200 OK\r\n") != NULL);
    assert(strstr(response, "\"status\":\"ok\"") != NULL);

    free(response);
    free(request);
}

static void test_get_tables_lists_schema_files(void)
{
    char original_cwd[PATH_MAX];
    char temp_dir[] = "/tmp/test_http_tablesXXXXXX";
    char data_dir[PATH_MAX];
    char schema_dir[PATH_MAX];
    char alpha_schema[PATH_MAX];
    char ignored_file[PATH_MAX];
    char *request;
    char *response;

    assert(getcwd(original_cwd, sizeof(original_cwd)) != NULL);
    assert(mkdtemp(temp_dir) != NULL);

    assert(snprintf(data_dir, sizeof(data_dir), "%s/data", temp_dir) > 0);
    assert(snprintf(schema_dir, sizeof(schema_dir), "%s/data/schema", temp_dir) > 0);
    assert(snprintf(alpha_schema, sizeof(alpha_schema), "%s/alpha.schema", schema_dir) > 0);
    assert(snprintf(ignored_file, sizeof(ignored_file), "%s/ignored.txt", schema_dir) > 0);

    assert(mkdir(data_dir, 0775) == 0);
    assert(mkdir(schema_dir, 0775) == 0);
    create_file_with_contents(alpha_schema, "id,INT\nname,VARCHAR\n");
    create_file_with_contents(ignored_file, "should not appear\n");

    assert(chdir(temp_dir) == 0);

    request = build_request("GET", "/tables", "");
    response = run_request_and_collect_response(request);

    assert(response != NULL);
    assert(strstr(response, "HTTP/1.1 200 OK\r\n") != NULL);
    assert(strstr(response, "\"alpha\"") != NULL);
    assert(strstr(response, "ignored") == NULL);

    free(response);
    free(request);

    assert(chdir(original_cwd) == 0);
    assert(unlink(alpha_schema) == 0);
    assert(unlink(ignored_file) == 0);
    assert(rmdir(schema_dir) == 0);
    assert(rmdir(data_dir) == 0);
    assert(rmdir(temp_dir) == 0);
}

static void test_post_query_rejects_malformed_json(void)
{
    char *request = build_request("POST", "/query", "{\"sql\":123}");
    char *response = run_request_and_collect_response(request);

    assert(response != NULL);
    assert(strstr(response, "HTTP/1.1 400 Bad Request\r\n") != NULL);
    assert(strstr(response, "\"error\":\"bad request\"") != NULL);

    free(response);
    free(request);
}

static void test_post_query_rejects_oversized_body(void)
{
    enum { oversized_length = 1048577 };
    char request[256];
    int written;
    char *response;

    written = snprintf(request, sizeof(request),
                       "POST /query HTTP/1.1\r\n"
                       "Host: localhost\r\n"
                       "Content-Type: application/json\r\n"
                       "Content-Length: %d\r\n"
                       "\r\n",
                       oversized_length);
    assert(written > 0);
    assert((size_t)written < sizeof(request));

    response = run_request_and_collect_response(request);

    assert(response != NULL);
    assert(strstr(response, "HTTP/1.1 413 Payload Too Large\r\n") != NULL);
    assert(strstr(response, "\"error\":\"payload too large\"") != NULL);

    free(response);
}

static void test_post_query_returns_500_when_queue_push_fails(void)
{
    job_queue_t *queue;
    char *request = build_request("POST", "/query", "{\"sql\":\"SELECT 1\"}");
    char *response;

    with_queue(&queue);
    queue_shutdown(queue);

    response = run_request_and_collect_response(request);

    assert(response != NULL);
    assert(strstr(response, "HTTP/1.1 500 Internal Server Error\r\n") != NULL);
    assert(strstr(response, "\"error\":\"internal server error\"") != NULL);

    free(response);
    free(request);
    cleanup_queue(queue);
}

static void test_query_requires_post_method(void)
{
    char *request = build_request("GET", "/query", "");
    char *response = run_request_and_collect_response(request);

    assert(response != NULL);
    assert(strstr(response, "HTTP/1.1 405 Method Not Allowed\r\n") != NULL);
    assert(strstr(response, "\"error\":\"method not allowed\"") != NULL);

    free(response);
    free(request);
}

int main(void)
{
    test_post_query_enqueues_sql_job();
    test_get_health_returns_ok();
    test_get_tables_lists_schema_files();
    test_post_query_rejects_malformed_json();
    test_post_query_rejects_oversized_body();
    test_post_query_returns_500_when_queue_push_fails();
    test_query_requires_post_method();
    printf("test_http passed\n");
    return 0;
}
