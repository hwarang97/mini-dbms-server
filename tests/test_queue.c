#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "job_queue.h"

/* shutdown 전에 스레드가 실제로 대기 상태에 들어갔는지 맞추는 작은 배리어. */
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int count;
} start_latch_t;

/* 연속된 job 구간을 push하는 producer 설정. */
typedef struct {
    job_queue_t *queue;
    job_t *jobs;
    int start_index;
    int count;
} producer_args_t;

/* dequeue 순서를 그대로 기록하는 consumer 설정. */
typedef struct {
    job_queue_t *queue;
    int *output;
    int count;
} ordered_consumer_args_t;

/* 소비된 각 job이 정확히 한 번만 나왔는지 세는 consumer 설정. */
typedef struct {
    job_queue_t *queue;
    int *seen;
    int *consumed_total;
    pthread_mutex_t *seen_mutex;
} counting_consumer_args_t;

/* shutdown 시 깨어나야 하는 pop 대기 스레드 인자. */
typedef struct {
    job_queue_t *queue;
    start_latch_t *latch;
    job_t *result;
} pop_waiter_args_t;

/* shutdown 시작 후 실패해야 하는 push 대기 스레드 인자. */
typedef struct {
    job_queue_t *queue;
    start_latch_t *latch;
    job_t *job;
    int rc;
} push_waiter_args_t;

/* blocking wakeup 테스트에서 사용하는 latch를 초기화한다. */
static void start_latch_init(start_latch_t *latch)
{
    assert(pthread_mutex_init(&latch->mutex, NULL) == 0);
    assert(pthread_cond_init(&latch->cond, NULL) == 0);
    latch->count = 0;
}

/* 대기 테스트가 끝난 뒤 latch 자원을 정리한다. */
static void start_latch_destroy(start_latch_t *latch)
{
    assert(pthread_cond_destroy(&latch->cond) == 0);
    assert(pthread_mutex_destroy(&latch->mutex) == 0);
}

/* 대기 스레드가 실제 blocking 지점까지 도달했음을 기록한다. */
static void start_latch_signal(start_latch_t *latch)
{
    assert(pthread_mutex_lock(&latch->mutex) == 0);
    latch->count++;
    assert(pthread_cond_broadcast(&latch->cond) == 0);
    assert(pthread_mutex_unlock(&latch->mutex) == 0);
}

/* 모든 helper 스레드가 shutdown 트리거를 받을 준비가 될 때까지 기다린다. */
static void start_latch_wait_for(start_latch_t *latch, int target)
{
    assert(pthread_mutex_lock(&latch->mutex) == 0);
    while (latch->count < target) {
        assert(pthread_cond_wait(&latch->cond, &latch->mutex) == 0);
    }
    assert(pthread_mutex_unlock(&latch->mutex) == 0);
}

/* 정해진 개수의 job을 공유 큐에 push한다. */
static void *producer_thread(void *arg)
{
    producer_args_t *args = (producer_args_t *)arg;
    int i;

    for (i = 0; i < args->count; ++i) {
        assert(queue_push(args->queue, &args->jobs[args->start_index + i]) == 0);
    }

    return NULL;
}

/* 정해진 개수의 job을 pop하면서 dequeue 순서를 기록한다. */
static void *ordered_consumer_thread(void *arg)
{
    ordered_consumer_args_t *args = (ordered_consumer_args_t *)arg;
    int i;

    for (i = 0; i < args->count; ++i) {
        job_t *job = queue_pop(args->queue);
        assert(job != NULL);
        args->output[i] = job->client_fd;
    }

    return NULL;
}

/* shutdown까지 계속 소비하면서 각 job id가 한 번만 나왔는지 센다. */
static void *counting_consumer_thread(void *arg)
{
    counting_consumer_args_t *args = (counting_consumer_args_t *)arg;

    for (;;) {
        job_t *job = queue_pop(args->queue);
        if (job == NULL) {
            break;
        }

        assert(pthread_mutex_lock(args->seen_mutex) == 0);
        assert(job->client_fd >= 0);
        args->seen[job->client_fd]++;
        (*args->consumed_total)++;
        assert(pthread_mutex_unlock(args->seen_mutex) == 0);
    }

    return NULL;
}

/* pop에서 block시켜 shutdown 동작을 검증한다. */
static void *pop_waiter_thread(void *arg)
{
    pop_waiter_args_t *args = (pop_waiter_args_t *)arg;

    start_latch_signal(args->latch);
    args->result = queue_pop(args->queue);
    return NULL;
}

/* push에서 block시켜 shutdown 동작을 검증한다. */
static void *push_waiter_thread(void *arg)
{
    push_waiter_args_t *args = (push_waiter_args_t *)arg;

    start_latch_signal(args->latch);
    args->rc = queue_push(args->queue, args->job);
    return NULL;
}

/* producer 1개, consumer 1개에서 FIFO 순서가 유지되는지 확인한다. */
static void test_single_producer_single_consumer(void)
{
    enum { job_count = 128 };
    job_queue_t *queue = queue_init(8);
    pthread_t producer;
    pthread_t consumer;
    producer_args_t producer_args;
    ordered_consumer_args_t consumer_args;
    job_t jobs[job_count];
    int output[job_count];
    int i;

    assert(queue != NULL);

    for (i = 0; i < job_count; ++i) {
        jobs[i].client_fd = i;
        jobs[i].sql = NULL;
        output[i] = -1;
    }

    producer_args.queue = queue;
    producer_args.jobs = jobs;
    producer_args.start_index = 0;
    producer_args.count = job_count;

    consumer_args.queue = queue;
    consumer_args.output = output;
    consumer_args.count = job_count;

    assert(pthread_create(&producer, NULL, producer_thread, &producer_args) == 0);
    assert(pthread_create(&consumer, NULL, ordered_consumer_thread, &consumer_args) == 0);

    assert(pthread_join(producer, NULL) == 0);
    assert(pthread_join(consumer, NULL) == 0);

    for (i = 0; i < job_count; ++i) {
        assert(output[i] == i);
    }

    queue_destroy(queue);
}

/* 빈 큐에서 대기 중인 모든 consumer가 shutdown으로 깨어나는지 확인한다. */
static void test_shutdown_wakes_blocked_poppers(void)
{
    enum { waiter_count = 4 };
    job_queue_t *queue = queue_init(2);
    pthread_t threads[waiter_count];
    pop_waiter_args_t args[waiter_count];
    start_latch_t latch;
    int i;

    assert(queue != NULL);
    start_latch_init(&latch);

    for (i = 0; i < waiter_count; ++i) {
        args[i].queue = queue;
        args[i].latch = &latch;
        args[i].result = (job_t *)1;
        assert(pthread_create(&threads[i], NULL, pop_waiter_thread, &args[i]) == 0);
    }

    start_latch_wait_for(&latch, waiter_count);
    queue_shutdown(queue);

    for (i = 0; i < waiter_count; ++i) {
        assert(pthread_join(threads[i], NULL) == 0);
        assert(args[i].result == NULL);
    }

    start_latch_destroy(&latch);
    queue_destroy(queue);
}

/* 가득 찬 큐에서 대기 중인 모든 producer가 shutdown으로 깨어나는지 확인한다. */
static void test_shutdown_wakes_blocked_pushers(void)
{
    enum { waiter_count = 4 };
    job_queue_t *queue = queue_init(1);
    pthread_t threads[waiter_count];
    push_waiter_args_t args[waiter_count];
    start_latch_t latch;
    job_t first_job;
    job_t jobs[waiter_count];
    int i;

    assert(queue != NULL);
    start_latch_init(&latch);

    first_job.client_fd = 999;
    first_job.sql = NULL;
    assert(queue_push(queue, &first_job) == 0);

    for (i = 0; i < waiter_count; ++i) {
        jobs[i].client_fd = i;
        jobs[i].sql = NULL;
        args[i].queue = queue;
        args[i].latch = &latch;
        args[i].job = &jobs[i];
        args[i].rc = 0;
        assert(pthread_create(&threads[i], NULL, push_waiter_thread, &args[i]) == 0);
    }

    start_latch_wait_for(&latch, waiter_count);
    queue_shutdown(queue);

    for (i = 0; i < waiter_count; ++i) {
        assert(pthread_join(threads[i], NULL) == 0);
        assert(args[i].rc == -1);
    }

    assert(queue_pop(queue) == &first_job);
    assert(queue_pop(queue) == NULL);

    start_latch_destroy(&latch);
    queue_destroy(queue);
}

/* 다중 스레드 workload를 돌려 job 유실이나 중복이 없는지 확인한다. */
static void run_multi_producer_consumer_case(int producer_count, int consumer_count,
    int jobs_per_producer, size_t capacity)
{
    int total_jobs = producer_count * jobs_per_producer;
    job_queue_t *queue = queue_init(capacity);
    producer_args_t *producer_args;
    counting_consumer_args_t *consumer_args;
    pthread_t *producers;
    pthread_t *consumers;
    pthread_mutex_t seen_mutex;
    job_t *jobs;
    int *seen;
    int consumed_total = 0;
    int i;

    assert(queue != NULL);
    assert(pthread_mutex_init(&seen_mutex, NULL) == 0);

    jobs = (job_t *)calloc((size_t)total_jobs, sizeof(*jobs));
    seen = (int *)calloc((size_t)total_jobs, sizeof(*seen));
    producer_args = (producer_args_t *)calloc((size_t)producer_count, sizeof(*producer_args));
    consumer_args = (counting_consumer_args_t *)calloc((size_t)consumer_count, sizeof(*consumer_args));
    producers = (pthread_t *)calloc((size_t)producer_count, sizeof(*producers));
    consumers = (pthread_t *)calloc((size_t)consumer_count, sizeof(*consumers));

    assert(jobs != NULL);
    assert(seen != NULL);
    assert(producer_args != NULL);
    assert(consumer_args != NULL);
    assert(producers != NULL);
    assert(consumers != NULL);

    for (i = 0; i < total_jobs; ++i) {
        jobs[i].client_fd = i;
        jobs[i].sql = NULL;
    }

    for (i = 0; i < consumer_count; ++i) {
        consumer_args[i].queue = queue;
        consumer_args[i].seen = seen;
        consumer_args[i].consumed_total = &consumed_total;
        consumer_args[i].seen_mutex = &seen_mutex;
        assert(pthread_create(&consumers[i], NULL, counting_consumer_thread, &consumer_args[i]) == 0);
    }

    for (i = 0; i < producer_count; ++i) {
        producer_args[i].queue = queue;
        producer_args[i].jobs = jobs;
        producer_args[i].start_index = i * jobs_per_producer;
        producer_args[i].count = jobs_per_producer;
        assert(pthread_create(&producers[i], NULL, producer_thread, &producer_args[i]) == 0);
    }

    for (i = 0; i < producer_count; ++i) {
        assert(pthread_join(producers[i], NULL) == 0);
    }

    queue_shutdown(queue);

    for (i = 0; i < consumer_count; ++i) {
        assert(pthread_join(consumers[i], NULL) == 0);
    }

    assert(consumed_total == total_jobs);
    for (i = 0; i < total_jobs; ++i) {
        assert(seen[i] == 1);
    }

    free(consumers);
    free(producers);
    free(consumer_args);
    free(producer_args);
    free(seen);
    free(jobs);
    assert(pthread_mutex_destroy(&seen_mutex) == 0);
    queue_destroy(queue);
}

/* 비교적 작은 동시성 workload에서 유실/중복이 없는지 확인한다. */
static void test_multiple_producers_multiple_consumers(void)
{
    run_multi_producer_consumer_case(4, 4, 250, 32);
}

/* 더 큰 동시성 workload로 큐를 스트레스 테스트한다. */
static void test_stress_queue(void)
{
    run_multi_producer_consumer_case(4, 4, 2500, 64);
}

/* Part 4 큐 테스트 전체를 실행한다. */
int main(void)
{
    test_single_producer_single_consumer();
    test_shutdown_wakes_blocked_poppers();
    test_shutdown_wakes_blocked_pushers();
    test_multiple_producers_multiple_consumers();
    test_stress_queue();
    printf("test_queue passed\n");
    return 0;
}
