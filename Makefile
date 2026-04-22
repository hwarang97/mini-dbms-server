CC ?= gcc
CFLAGS ?= -Wall -Wextra -Wpedantic -std=c11 -g -pthread
CPPFLAGS ?= -Iinclude -Iexternal/cjson -Iexternal/db_engine

PROJECT_SOURCES = \
    src/main.c \
    src/server/server.c \
    src/http/http.c \
    src/pool/thread_pool.c \
    src/pool/db_wrapper.c \
    src/queue/job_queue.c

CJSON_SOURCES = external/cjson/cJSON.c

DB_ENGINE_SOURCES = \
    external/db_engine/ast_print.c \
    external/db_engine/bptree.c \
    external/db_engine/executor.c \
    external/db_engine/json_out.c \
    external/db_engine/parser.c \
    external/db_engine/sql_format.c \
    external/db_engine/sql_processor.c \
    external/db_engine/storage.c

SERVER_OBJECTS = $(PROJECT_SOURCES:%.c=obj/%.o) $(CJSON_SOURCES:%.c=obj/%.o) $(DB_ENGINE_SOURCES:%.c=obj/%.o)
TEST_SUPPORT_OBJECTS = $(filter-out obj/src/main.o,$(SERVER_OBJECTS))
TEST_SOURCES = tests/test_server.c tests/test_http.c tests/test_pool.c tests/test_queue.c
TEST_BINS = $(TEST_SOURCES:tests/%.c=bin/%)
RESOURCE_CHECK_SCRIPTS = \
	scripts/check_leaks.sh \
	scripts/check_races.sh \
	scripts/check_fd.sh
E2E_SCRIPT = scripts/run_e2e.sh

.PHONY: all clean test e2e check-leaks check-races check-fd check-resources

all: bin/dbms_server

test: $(TEST_BINS)
	@set -e; for test_bin in $(TEST_BINS); do ./$$test_bin; done

e2e: bin/dbms_server $(E2E_SCRIPT)
	bash $(E2E_SCRIPT)

check-leaks: bin/dbms_server $(RESOURCE_CHECK_SCRIPTS)
	bash scripts/check_leaks.sh

check-races: bin/dbms_server $(RESOURCE_CHECK_SCRIPTS)
	bash scripts/check_races.sh

check-fd: bin/dbms_server $(RESOURCE_CHECK_SCRIPTS)
	bash scripts/check_fd.sh

check-resources: check-leaks check-races check-fd

bin/dbms_server: $(SERVER_OBJECTS)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $^ -o $@

bin/%: obj/tests/%.o $(TEST_SUPPORT_OBJECTS)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $^ -o $@

obj/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

clean:
	rm -rf bin obj
