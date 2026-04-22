#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "job_queue.h"
#include "server.h"
#include "thread_pool.h"

enum {
    DEFAULT_PORT = 8080,
    DEFAULT_QUEUE_CAPACITY = 128,
    DEFAULT_WORKER_COUNT = 8
};

typedef struct {
    sigset_t signal_set;
} signal_thread_args_t;

static void shutdown_runtime(void)
{
    if (g_queue != NULL) {
        queue_shutdown(g_queue);
    }

    pool_destroy();

    if (g_queue != NULL) {
        queue_destroy(g_queue);
        g_queue = NULL;
    }
}

static int parse_port_argument(int argc, char **argv, int *out_port)
{
    char *parse_end = NULL;
    long parsed_port = 0;

    if (argc == 1) {
        *out_port = DEFAULT_PORT;
        return 0;
    }

    if (argc != 2) {
        fprintf(stderr, "Usage: %s [port]\n", argv[0]);
        return -1;
    }

    errno = 0;
    parsed_port = strtol(argv[1], &parse_end, 10);
    if (errno != 0 || parse_end == argv[1] || *parse_end != '\0') {
        fprintf(stderr, "Invalid port: %s\n", argv[1]);
        return -1;
    }

    if (parsed_port <= 0 || parsed_port > 65535) {
        fprintf(stderr, "Port must be between 1 and 65535\n");
        return -1;
    }

    *out_port = (int)parsed_port;
    return 0;
}

static int ignore_sigpipe(void)
{
    struct sigaction action;

    memset(&action, 0, sizeof(action));
    action.sa_handler = SIG_IGN;

    if (sigaction(SIGPIPE, &action, NULL) != 0) {
        perror("sigaction(SIGPIPE)");
        return -1;
    }

    return 0;
}

static void *signal_wait_thread_main(void *arg)
{
    const signal_thread_args_t *thread_args = (const signal_thread_args_t *)arg;
    int signal_number = 0;
    int wait_status = sigwait(&thread_args->signal_set, &signal_number);

    if (wait_status != 0) {
        fprintf(stderr, "sigwait failed: %s\n", strerror(wait_status));
        return NULL;
    }

    fprintf(stderr, "Received signal %d, stopping server\n", signal_number);
    server_stop();
    return NULL;
}

int main(int argc, char **argv)
{
    int status;
    int port = DEFAULT_PORT;
    int exit_code = 1;
    int server_status;
    sigset_t signal_set;
    pthread_t signal_thread;
    signal_thread_args_t signal_thread_args;
    int signal_thread_started = 0;

    if (parse_port_argument(argc, argv, &port) != 0) {
        return 1;
    }

    if (ignore_sigpipe() != 0) {
        return 1;
    }

    sigemptyset(&signal_set);
    sigaddset(&signal_set, SIGINT);
    sigaddset(&signal_set, SIGTERM);
    status = pthread_sigmask(SIG_BLOCK, &signal_set, NULL);
    if (status != 0) {
        fprintf(stderr, "pthread_sigmask failed: %s\n", strerror(status));
        return 1;
    }

    g_queue = queue_init(DEFAULT_QUEUE_CAPACITY);
    if (g_queue == NULL) {
        fprintf(stderr, "queue_init failed\n");
        return 1;
    }

    if (pool_init(DEFAULT_WORKER_COUNT, g_queue) != 0) {
        fprintf(stderr, "pool_init failed\n");
        queue_destroy(g_queue);
        g_queue = NULL;
        return 1;
    }

    signal_thread_args.signal_set = signal_set;
    status = pthread_create(&signal_thread, NULL, signal_wait_thread_main, &signal_thread_args);
    if (status != 0) {
        fprintf(stderr, "pthread_create(signal thread) failed: %s\n", strerror(status));
        shutdown_runtime();
        return 1;
    }
    signal_thread_started = 1;

    server_status = server_start(port);
    if (server_status != 0) {
        fprintf(stderr, "server_start failed\n");
    }

    if (signal_thread_started) {
        (void)pthread_kill(signal_thread, SIGTERM);
        (void)pthread_join(signal_thread, NULL);
    }

    shutdown_runtime();

    if (server_status == 0) {
        exit_code = 0;
    }

    return exit_code;
}
