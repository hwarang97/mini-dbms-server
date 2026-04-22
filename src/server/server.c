#include "server.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#include "http.h"

static volatile sig_atomic_t g_stop_requested = 0;
static int g_listen_fd = -1;

static void close_listen_socket(void)
{
    if (g_listen_fd >= 0) {
        close(g_listen_fd);
        g_listen_fd = -1;
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

    g_stop_requested = 0;
    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_listen_fd < 0) {
        perror("socket");
        return -1;
    }

    if (setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_addr,
                   sizeof(reuse_addr)) < 0) {
        perror("setsockopt");
        close_listen_socket();
        return -1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons((uint16_t)port);

    if (bind(g_listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close_listen_socket();
        return -1;
    }

    if (listen(g_listen_fd, SOMAXCONN) < 0) {
        perror("listen");
        close_listen_socket();
        return -1;
    }

    fprintf(stderr, "Server listening on port %d\n", port);

    while (!g_stop_requested) {
        int client_fd = accept(g_listen_fd, NULL, NULL);

        if (client_fd < 0) {
            if (errno == EINTR) {
                if (g_stop_requested) {
                    break;
                }
                continue;
            }

            if (g_stop_requested) {
                break;
            }

            perror("accept");
            continue;
        }

        fprintf(stderr, "Accepted connection on port %d\n", port);
        handle_connection(client_fd);
    }

    close_listen_socket();
    return 0;
}

void server_stop(void)
{
    g_stop_requested = 1;
    close_listen_socket();
}
