#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "thread_pool.h"

void pool_set_queue_hooks_for_test(job_t *(*pop_fn)(job_queue_t *),
                                   void (*shutdown_fn)(job_queue_t *));
void pool_set_query_executor_for_test(int (*executor_fn)(const char *sql, char **out_json));

#define TEST_JOB_COUNT 24
#define TEST_WORKER_COUNT 4

enum mock_mode {
    MOCK_DRAIN_JOBS,
    MOCK_BLOCK_UNTIL_SHUTDOWN
};

static pthread_mutex_t mock_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t mock_cond = PTHREAD_COND_INITIALIZER;
static enum mock_mode mock_mode_value = MOCK_DRAIN_JOBS;
static job_t *mock_jobs[TEST_JOB_COUNT];
static int mock_write_fds[TEST_JOB_COUNT];
static int mock_read_fds[TEST_JOB_COUNT];
static size_t mock_job_count = 0;
static size_t mock_next_job = 0;
static int mock_shutdown_seen = 0;
static unsigned int mock_shutdown_calls = 0;
static int fake_queue_token = 0;

static int mock_execute_query(const char *sql, char **out_json)
{
    const char *body = "{\"status\":\"ok\"}";
    size_t len = strlen(body) + 1U;

    (void)sql;

    *out_json = (char *)malloc(len);
    assert(*out_json != NULL);
    memcpy(*out_json, body, len);
    return 0;
}

static char *duplicate_string(const char *value)
{
    size_t len = strlen(value) + 1;
    char *copy = (char *)malloc(len); /* worker frees as job->sql */

    assert(copy != NULL);
    memcpy(copy, value, len);
    return copy;
}

static job_t *make_job(size_t index)
{
    const char *sql = "SELECT * FROM users";
    int pipe_fds[2];
    job_t *job;

    assert(pipe(pipe_fds) == 0);

    job = (job_t *)malloc(sizeof(*job)); /* worker frees after process_job */
    assert(job != NULL);
    job->client_fd = pipe_fds[1];
    job->sql = duplicate_string(sql); /* worker frees after process_job */

    mock_read_fds[index] = pipe_fds[0];
    mock_write_fds[index] = pipe_fds[1];
    return job;
}

static void reset_mock(enum mock_mode mode)
{
    size_t i;

    mock_mode_value = mode;
    mock_job_count = 0;
    mock_next_job = 0;
    mock_shutdown_seen = 0;
    mock_shutdown_calls = 0;

    for (i = 0; i < TEST_JOB_COUNT; ++i) {
        mock_jobs[i] = NULL;
        mock_read_fds[i] = -1;
        mock_write_fds[i] = -1;
    }
}

static job_t *mock_queue_pop(job_queue_t *queue)
{
    job_t *job = NULL;

    (void)queue;

    pthread_mutex_lock(&mock_mutex);

    if (mock_mode_value == MOCK_BLOCK_UNTIL_SHUTDOWN) {
        while (!mock_shutdown_seen) {
            pthread_cond_wait(&mock_cond, &mock_mutex);
        }
    } else if (mock_next_job < mock_job_count) {
        job = mock_jobs[mock_next_job];
        mock_jobs[mock_next_job] = NULL;
        ++mock_next_job;
    }

    pthread_mutex_unlock(&mock_mutex);
    return job;
}

static void mock_queue_shutdown(job_queue_t *queue)
{
    (void)queue;

    pthread_mutex_lock(&mock_mutex);
    mock_shutdown_seen = 1;
    ++mock_shutdown_calls;
    pthread_cond_broadcast(&mock_cond);
    pthread_mutex_unlock(&mock_mutex);
}

static void prepare_jobs(size_t count)
{
    size_t i;

    assert(count <= TEST_JOB_COUNT);
    mock_job_count = count;

    for (i = 0; i < count; ++i) {
        mock_jobs[i] = make_job(i);
    }
}

static void assert_worker_closed_fd(int fd)
{
    errno = 0;
    assert(fcntl(fd, F_GETFD) == -1);
    assert(errno == EBADF);
}

static void close_read_fds(size_t count)
{
    size_t i;

    for (i = 0; i < count; ++i) {
        if (mock_read_fds[i] >= 0) {
            close(mock_read_fds[i]);
            mock_read_fds[i] = -1;
        }
    }
}

static char *read_all_from_fd(int fd)
{
    size_t capacity = 512;
    size_t length = 0;
    char *buffer = (char *)malloc(capacity);

    assert(buffer != NULL);

    for (;;) {
        ssize_t bytes_read;
        char *grown_buffer;

        if (length + 256 >= capacity) {
            capacity *= 2;
            grown_buffer = (char *)realloc(buffer, capacity);
            assert(grown_buffer != NULL);
            buffer = grown_buffer;
        }

        bytes_read = read(fd, buffer + length, capacity - length - 1);
        assert(bytes_read >= 0);

        if (bytes_read == 0) {
            break;
        }

        length += (size_t)bytes_read;
    }

    buffer[length] = '\0';
    return buffer;
}

static char *run_job_and_collect_response(const char *sql)
{
    int pipe_fds[2];
    job_queue_t *queue;
    job_t *job;
    char *response;

    assert(pipe(pipe_fds) == 0);

    queue = queue_init(4);
    assert(queue != NULL);

    job = (job_t *)malloc(sizeof(*job)); /* worker frees after process_job */
    assert(job != NULL);
    job->client_fd = pipe_fds[1];
    job->sql = duplicate_string(sql); /* worker frees after process_job */

    pool_set_queue_hooks_for_test(NULL, NULL);
    pool_set_query_executor_for_test(NULL);

    assert(pool_init(1, queue) == 0);
    assert(queue_push(queue, job) == 0);

    pool_shutdown();
    response = read_all_from_fd(pipe_fds[0]);

    assert_worker_closed_fd(pipe_fds[1]);

    close(pipe_fds[0]);
    pool_destroy();
    queue_destroy(queue);

    return response;
}

static void test_pool_init_rejects_invalid_arguments(void)
{
    job_queue_t *fake_queue = (job_queue_t *)&fake_queue_token;

    reset_mock(MOCK_BLOCK_UNTIL_SHUTDOWN);
    pool_set_queue_hooks_for_test(mock_queue_pop, mock_queue_shutdown);

    assert(pool_init(0, fake_queue) == -1);
    assert(pool_init(TEST_WORKER_COUNT, NULL) == -1);

    assert(pool_init(TEST_WORKER_COUNT, fake_queue) == 0);
    assert(pool_init(1, fake_queue) == -1);

    pool_shutdown();
    pool_destroy();
}

static void test_workers_process_jobs(void)
{
    size_t i;
    job_queue_t *fake_queue = (job_queue_t *)&fake_queue_token;

    reset_mock(MOCK_DRAIN_JOBS);
    prepare_jobs(TEST_JOB_COUNT);
    pool_set_queue_hooks_for_test(mock_queue_pop, mock_queue_shutdown);
    pool_set_query_executor_for_test(mock_execute_query);

    assert(pool_init(TEST_WORKER_COUNT, fake_queue) == 0);
    pool_shutdown();

    assert(mock_shutdown_calls == 1);
    assert(mock_next_job == TEST_JOB_COUNT);

    for (i = 0; i < TEST_JOB_COUNT; ++i) {
        assert_worker_closed_fd(mock_write_fds[i]);
    }

    close_read_fds(TEST_JOB_COUNT);
    pool_destroy();
}

static void test_worker_writes_success_response(void)
{
    char *response = run_job_and_collect_response("SELECT * FROM users");

    assert(strstr(response, "HTTP/1.1 200 OK\r\n") != NULL);
    assert(strstr(response, "Content-Type: application/json\r\n") != NULL);
    assert(strstr(response, "\"status\":\"ok\"") != NULL);
    assert(strstr(response, "\"rows\":[") != NULL);
    assert(strstr(response, "\"alice\"") != NULL);

    free(response);
}

static void test_worker_writes_error_response_on_query_failure(void)
{
    char *response = run_job_and_collect_response("THIS IS NOT VALID SQL");

    assert(strstr(response, "HTTP/1.1 500 Internal Server Error\r\n") != NULL);
    assert(strstr(response, "Content-Length: 2\r\n") != NULL);
    assert(strstr(response, "\r\n\r\n{}") != NULL);

    free(response);
}

static void test_shutdown_joins_idle_workers(void)
{
    job_queue_t *fake_queue = (job_queue_t *)&fake_queue_token;

    reset_mock(MOCK_BLOCK_UNTIL_SHUTDOWN);
    pool_set_queue_hooks_for_test(mock_queue_pop, mock_queue_shutdown);
    pool_set_query_executor_for_test(mock_execute_query);

    assert(pool_init(TEST_WORKER_COUNT, fake_queue) == 0);
    pool_shutdown();

    assert(mock_shutdown_calls == 1);
    assert(mock_shutdown_seen == 1);
    pool_destroy();
}

int main(void)
{
    test_pool_init_rejects_invalid_arguments();
    test_workers_process_jobs();
    test_worker_writes_success_response();
    test_worker_writes_error_response_on_query_failure();
    test_shutdown_joins_idle_workers();
    pool_set_queue_hooks_for_test(NULL, NULL);
    pool_set_query_executor_for_test(NULL);

    printf("test_pool passed\n");
    return 0;
}
