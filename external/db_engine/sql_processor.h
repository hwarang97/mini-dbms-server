#ifndef SQL_PROCESSOR_H
#define SQL_PROCESSOR_H

#include "types.h"

typedef struct SQLProcessorOptions {
    int debug_mode;
    int json_mode;
    int tokens_mode;
    int format_mode;
} SQLProcessorOptions;

/* 기존 week6 SQL 처리기 연결 지점을 위한 헤더.
 * 실제 파서/실행 코드는 parser.c, executor.c, storage.c를 재활용한다. */
int sql_processor_status(void);
int sql_processor_run_file(const char *path, SQLProcessorOptions options);
int sql_processor_run_string(const char *input, SQLProcessorOptions options);

#endif
