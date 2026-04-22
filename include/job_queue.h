#ifndef JOB_QUEUE_H
#define JOB_QUEUE_H

#include <stddef.h>

#include "common.h"

job_queue_t *queue_init(size_t capacity);
int queue_push(job_queue_t *q, job_t *job);
job_t *queue_pop(job_queue_t *q);
void queue_shutdown(job_queue_t *q);
void queue_destroy(job_queue_t *q);

#endif /* JOB_QUEUE_H */
