#include "thread_pool.h"

int pool_init(size_t num_workers, job_queue_t *queue)
{
    (void)num_workers;
    (void)queue;
    return 0;
}

void pool_shutdown(void)
{
}

void pool_destroy(void)
{
}
