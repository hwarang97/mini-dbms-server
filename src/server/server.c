#include "server.h"

int server_start(int port)
{
    return (port < 0) ? -1 : 0;
}

void server_stop(void)
{
}
