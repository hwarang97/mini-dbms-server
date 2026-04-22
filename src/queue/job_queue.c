#include "job_queue.h"

#include <pthread.h>
#include <stdlib.h>

/* 하나의 mutex와 두 개의 조건변수로 보호되는 고정 크기 링 버퍼 큐. */
struct job_queue {
    job_t **buffer;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    int shutdown;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
};

job_queue_t *g_queue = NULL;

/* 큐 저장공간을 할당하고 동기화 객체를 초기화한다. */
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

    q->buffer = (job_t **)calloc(capacity, sizeof(*q->buffer));
    if (q->buffer == NULL) {
        free(q);
        return NULL;
    }

    q->capacity = capacity;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->shutdown = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        free(q->buffer);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->mutex);
        free(q->buffer);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_full, NULL) != 0) {
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        free(q->buffer);
        free(q);
        return NULL;
    }

    return q;
}

/* 큐가 가득 찼으면 대기하고, shutdown 이후에는 push를 거부한다. */
int queue_push(job_queue_t *q, job_t *job)
{
    if (q == NULL || job == NULL) {
        return -1;
    }

    pthread_mutex_lock(&q->mutex);

    while (q->count == q->capacity && !q->shutdown) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }

    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    q->buffer[q->tail] = job;
    q->tail = (q->tail + 1U) % q->capacity;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* 큐가 비어 있으면 대기하고, shutdown 후 비워진 경우에만 NULL을 반환한다. */
job_t *queue_pop(job_queue_t *q)
{
    job_t *job;

    if (q == NULL) {
        return NULL;
    }

    pthread_mutex_lock(&q->mutex);

    while (q->count == 0 && !q->shutdown) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }

    if (q->count == 0 && q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }

    job = q->buffer[q->head];
    q->buffer[q->head] = NULL;
    q->head = (q->head + 1U) % q->capacity;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return job;
}

/* 대기 중인 producer와 consumer를 모두 깨워 shutdown을 관찰하게 한다. */
void queue_shutdown(job_queue_t *q)
{
    if (q == NULL) {
        return;
    }

    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}

/* 모든 producer/consumer가 멈춘 뒤 큐가 소유한 자원을 해제한다. */
void queue_destroy(job_queue_t *q)
{
    if (q == NULL) {
        return;
    }

    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mutex);
    free(q->buffer);
    free(q);
}
