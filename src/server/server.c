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

/* 서버 실행/종료가 서로 다른 흐름에서 접근할 수 있어 원자적으로 관리한다. */
static atomic_int g_stop_requested = ATOMIC_VAR_INIT(0);
static atomic_int g_listen_fd = ATOMIC_VAR_INIT(-1);
/* 테스트에서는 mock handler로 바꿔 끼우고, 기본값은 실제 handle_connection이다. */
static void (*g_connection_handler)(int) = handle_connection;

static void close_listen_socket(void)
{
    /* 이미 닫힌 소켓을 다시 닫지 않도록 fd를 한 번에 회수한다. */
    int listen_fd = atomic_exchange(&g_listen_fd, -1);

    if (listen_fd >= 0)
    {
        close(listen_fd);
    }
}

static void shutdown_listen_socket(void)
{
    /* blocking accept를 깨우기 위해 shutdown 후 close까지 같이 수행한다. */
    int listen_fd = atomic_exchange(&g_listen_fd, -1);

    if (listen_fd >= 0)
    {
        shutdown(listen_fd, SHUT_RDWR);
        close(listen_fd);
    }
}

int server_start(int port)
{
    int reuse_addr = 1;             // 서버 리부팅시 포트 사용 정보가 남지 않는 기능 키기/끄기.
    struct sockaddr_in server_addr; // port binding 시 필요한 ip, port 저장 구조체

    if (port <= 0 || port > 65535)
    {
        fprintf(stderr, "Invalid server port: %d\n", port);
        return -1;
    }

    /* 새로 시작하는 서버이므로 종료 플래그를 초기화한다. */
    atomic_store(&g_stop_requested, 0);
    atomic_store(&g_listen_fd, socket(AF_INET, SOCK_STREAM, 0));
    if (atomic_load(&g_listen_fd) < 0)
    {
        perror("socket");
        return -1;
    }

    if (setsockopt(atomic_load(&g_listen_fd), SOL_SOCKET, SO_REUSEADDR, &reuse_addr,
                   sizeof(reuse_addr)) < 0)
    {
        perror("setsockopt");
        close_listen_socket();
        return -1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons((uint16_t)port);

    /* 모든 인터페이스에서 지정 포트로 접속을 받는다. */
    if (bind(atomic_load(&g_listen_fd), (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0)
    {
        perror("bind");
        close_listen_socket();
        return -1;
    }

    if (listen(atomic_load(&g_listen_fd), SOMAXCONN) < 0)
    {
        perror("listen");
        close_listen_socket();
        return -1;
    }

    fprintf(stderr, "Server listening on port %d\n", port);

    /* 종료 요청이 오기 전까지 새 연결을 받아 HTTP 단계로 넘긴다. */
    while (!atomic_load(&g_stop_requested))
    {
        int listen_fd = atomic_load(&g_listen_fd);
        int client_fd;

        if (listen_fd < 0)
        {
            break;
        }

        client_fd = accept(listen_fd, NULL, NULL);

        if (client_fd < 0)
        {
            if (errno == EINTR)
            {
                /* 시그널로 잠시 끊긴 경우에는 종료 여부만 확인하고 다시 대기한다. */
                if (atomic_load(&g_stop_requested))
                {
                    break;
                }
                continue;
            }

            /* server_stop이 먼저 리스닝 소켓을 정리했으면 정상 종료로 본다. */
            if (atomic_load(&g_stop_requested))
            {
                break;
            }

            perror("accept");
            continue;
        }

        fprintf(stderr, "Accepted connection on port %d\n", port);
        /* client_fd 소유권은 handle_connection 이후 단계로 넘어간다. */
        g_connection_handler(client_fd);
    }

    close_listen_socket();
    return 0;
}

void server_stop(void)
{
    /* 다음 루프 반복 전에 종료 상태를 먼저 알린다. */
    atomic_store(&g_stop_requested, 1);
    /* accept가 깨어나야 server_start가 루프를 빠져나올 수 있다. */
    shutdown_listen_socket();
}

void server_set_connection_handler_for_test(void (*handler)(int))
{
    /* 테스트에서는 mock을, 평소에는 실제 handler를 사용한다. */
    g_connection_handler = (handler != NULL) ? handler : handle_connection;
}
