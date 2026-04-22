#include "job_queue.h"

#include <stdlib.h>

struct job_queue {
    size_t capacity;
};

job_queue_t *g_queue = NULL;

job_queue_t *queue_init(size_t capacity)
{
    job_queue_t *q;

    if (capacity == 0) {
        return NULL;
    }

    q = (job_queue_t *)malloc(sizeof(*q));
    if (q == NULL) {
        return NULL;
    }

    q->capacity = capacity;
    return q;
}

int queue_push(job_queue_t *q, job_t *job)
{
    (void)q;
    (void)job;
    return 0;
}

job_t *queue_pop(job_queue_t *q)
{
    (void)q;
    return NULL;
}

void queue_shutdown(job_queue_t *q)
{
    (void)q;
}

void queue_destroy(job_queue_t *q)
{
    free(q);
}
