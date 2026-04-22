#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <stddef.h>

#include "job_queue.h"

int pool_init(size_t num_workers, job_queue_t *queue);
void pool_shutdown(void);
void pool_destroy(void);

#endif /* THREAD_POOL_H */
