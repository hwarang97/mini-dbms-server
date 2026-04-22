#include "server.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#include "http.h"

static atomic_int g_stop_requested = ATOMIC_VAR_INIT(0);
static atomic_int g_listen_fd = ATOMIC_VAR_INIT(-1);
static void (*g_connection_handler)(int) = handle_connection;

static void close_listen_socket(void)
{
    int listen_fd = atomic_exchange(&g_listen_fd, -1);

    if (listen_fd >= 0) {
        close(listen_fd);
    }
}

static void shutdown_listen_socket(void)
{
    int listen_fd = atomic_exchange(&g_listen_fd, -1);

    if (listen_fd >= 0) {
        shutdown(listen_fd, SHUT_RDWR);
        close(listen_fd);
    }
}

int server_start(int port)
{
    int reuse_addr = 1;
    struct sockaddr_in server_addr;

    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Invalid server port: %d\n", port);
        return -1;
    }

    atomic_store(&g_stop_requested, 0);
    atomic_store(&g_listen_fd, socket(AF_INET, SOCK_STREAM, 0));
    if (atomic_load(&g_listen_fd) < 0) {
        perror("socket");
        return -1;
    }

    if (setsockopt(atomic_load(&g_listen_fd), SOL_SOCKET, SO_REUSEADDR, &reuse_addr,
                   sizeof(reuse_addr)) < 0) {
        perror("setsockopt");
        close_listen_socket();
        return -1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons((uint16_t)port);

    if (bind(atomic_load(&g_listen_fd), (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0) {
        perror("bind");
        close_listen_socket();
        return -1;
    }

    if (listen(atomic_load(&g_listen_fd), SOMAXCONN) < 0) {
        perror("listen");
        close_listen_socket();
        return -1;
    }

    fprintf(stderr, "Server listening on port %d\n", port);

    while (!atomic_load(&g_stop_requested)) {
        int listen_fd = atomic_load(&g_listen_fd);
        int client_fd;

        if (listen_fd < 0) {
            break;
        }

        client_fd = accept(listen_fd, NULL, NULL);

        if (client_fd < 0) {
            if (errno == EINTR) {
                if (atomic_load(&g_stop_requested)) {
                    break;
                }
                continue;
            }

            if (atomic_load(&g_stop_requested)) {
                break;
            }

            perror("accept");
            continue;
        }

        fprintf(stderr, "Accepted connection on port %d\n", port);
        g_connection_handler(client_fd);
    }

    close_listen_socket();
    return 0;
}

void server_stop(void)
{
    atomic_store(&g_stop_requested, 1);
    shutdown_listen_socket();
}

void server_set_connection_handler_for_test(void (*handler)(int))
{
    g_connection_handler = (handler != NULL) ? handler : handle_connection;
}
