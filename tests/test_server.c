#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <time.h>
#include <unistd.h>

#include "server.h"

extern void server_set_connection_handler_for_test(void (*handler)(int));

typedef struct {
    pthread_mutex_t mutex;
    int call_count;
    int last_fd;
} mock_state_t;

typedef struct {
    int port;
    int result;
} server_thread_args_t;

static mock_state_t g_mock_state = {
    PTHREAD_MUTEX_INITIALIZER,
    0,
    -1
};

static void sleep_milliseconds(long milliseconds)
{
    struct timespec delay;

    delay.tv_sec = milliseconds / 1000;
    delay.tv_nsec = (milliseconds % 1000) * 1000000L;
    nanosleep(&delay, NULL);
}

static void reset_mock_state(void)
{
    int rc = pthread_mutex_lock(&g_mock_state.mutex);
    assert(rc == 0);
    g_mock_state.call_count = 0;
    g_mock_state.last_fd = -1;
    rc = pthread_mutex_unlock(&g_mock_state.mutex);
    assert(rc == 0);
}

static int get_mock_call_count(void)
{
    int call_count;
    int rc = pthread_mutex_lock(&g_mock_state.mutex);
    assert(rc == 0);
    call_count = g_mock_state.call_count;
    rc = pthread_mutex_unlock(&g_mock_state.mutex);
    assert(rc == 0);
    return call_count;
}

static int get_mock_last_fd(void)
{
    int last_fd;
    int rc = pthread_mutex_lock(&g_mock_state.mutex);
    assert(rc == 0);
    last_fd = g_mock_state.last_fd;
    rc = pthread_mutex_unlock(&g_mock_state.mutex);
    assert(rc == 0);
    return last_fd;
}

static void mock_handle_connection(int client_fd)
{
    int rc = pthread_mutex_lock(&g_mock_state.mutex);
    assert(rc == 0);
    g_mock_state.call_count += 1;
    g_mock_state.last_fd = client_fd;
    rc = pthread_mutex_unlock(&g_mock_state.mutex);
    assert(rc == 0);

    close(client_fd);
}

static void *server_thread_main(void *arg)
{
    server_thread_args_t *thread_args = arg;
    thread_args->result = server_start(thread_args->port);
    return NULL;
}

static int reserve_test_port(void)
{
    int sock_fd;
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(sock_fd >= 0);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);

    assert(bind(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    assert(getsockname(sock_fd, (struct sockaddr *)&addr, &addr_len) == 0);

    close(sock_fd);
    return ntohs(addr.sin_port);
}

static int connect_with_retry(int port)
{
    int attempt;

    for (attempt = 0; attempt < 200; ++attempt) {
        int client_fd;
        int connect_result;
        struct sockaddr_in server_addr;

        client_fd = socket(AF_INET, SOCK_STREAM, 0);
        assert(client_fd >= 0);

        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        server_addr.sin_port = htons((uint16_t)port);

        connect_result = connect(client_fd, (struct sockaddr *)&server_addr,
                                 sizeof(server_addr));
        if (connect_result == 0) {
            return client_fd;
        }

        close(client_fd);
        sleep_milliseconds(10);
    }

    return -1;
}

static void start_server(pthread_t *thread, server_thread_args_t *thread_args, int port)
{
    reset_mock_state();
    server_set_connection_handler_for_test(mock_handle_connection);

    thread_args->port = port;
    thread_args->result = -1;

    assert(pthread_create(thread, NULL, server_thread_main, thread_args) == 0);
}

static void stop_server(pthread_t thread, server_thread_args_t *thread_args)
{
    alarm(5);
    server_stop();
    assert(pthread_join(thread, NULL) == 0);
    alarm(0);

    assert(thread_args->result == 0);
    server_set_connection_handler_for_test(NULL);
}

static void wait_for_handler_calls(int expected_calls)
{
    int attempt;

    for (attempt = 0; attempt < 200; ++attempt) {
        if (get_mock_call_count() >= expected_calls) {
            return;
        }
        sleep_milliseconds(10);
    }

    assert(!"Timed out waiting for handle_connection to be called");
}

static void test_server_binds_to_the_requested_port(void)
{
    int probe_fd;
    int port = reserve_test_port();
    pthread_t server_thread;
    server_thread_args_t thread_args;
    struct sockaddr_in probe_addr;

    start_server(&server_thread, &thread_args, port);

    probe_fd = connect_with_retry(port);
    assert(probe_fd >= 0);
    close(probe_fd);
    wait_for_handler_calls(1);

    probe_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(probe_fd >= 0);

    memset(&probe_addr, 0, sizeof(probe_addr));
    probe_addr.sin_family = AF_INET;
    probe_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    probe_addr.sin_port = htons((uint16_t)port);

    errno = 0;
    assert(bind(probe_fd, (struct sockaddr *)&probe_addr, sizeof(probe_addr)) < 0);
    assert(errno == EADDRINUSE);

    close(probe_fd);
    stop_server(server_thread, &thread_args);
}

static void test_accept_loop_delivers_connection_fds(void)
{
    int client_fd;
    int port = reserve_test_port();
    pthread_t server_thread;
    server_thread_args_t thread_args;

    start_server(&server_thread, &thread_args, port);

    client_fd = connect_with_retry(port);
    assert(client_fd >= 0);
    close(client_fd);

    wait_for_handler_calls(1);
    assert(get_mock_last_fd() >= 0);

    stop_server(server_thread, &thread_args);
}

static void test_server_stop_breaks_the_accept_loop(void)
{
    int client_fd;
    int probe_fd;
    int port = reserve_test_port();
    pthread_t server_thread;
    server_thread_args_t thread_args;
    struct sockaddr_in probe_addr;

    start_server(&server_thread, &thread_args, port);

    client_fd = connect_with_retry(port);
    assert(client_fd >= 0);
    close(client_fd);
    wait_for_handler_calls(1);

    stop_server(server_thread, &thread_args);

    probe_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(probe_fd >= 0);

    memset(&probe_addr, 0, sizeof(probe_addr));
    probe_addr.sin_family = AF_INET;
    probe_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    probe_addr.sin_port = htons((uint16_t)port);

    assert(bind(probe_fd, (struct sockaddr *)&probe_addr, sizeof(probe_addr)) == 0);
    close(probe_fd);
}

int main(void)
{
    test_server_binds_to_the_requested_port();
    test_accept_loop_delivers_connection_fds();
    test_server_stop_breaks_the_accept_loop();
    printf("test_server passed\n");
    return 0;
}
