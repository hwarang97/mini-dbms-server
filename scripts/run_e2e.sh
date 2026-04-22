#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVER_BIN="${PROJECT_ROOT}/bin/dbms_server"
RUN_PARENT="${PROJECT_ROOT}/test-results/e2e"
RUN_DIR=""
SERVER_PID=""
SERVER_LOGGER_PID=""
SERVER_PIPE=""

if [[ -t 1 ]] && [[ -z "${NO_COLOR:-}" ]]; then
    COLOR_RESET=$'\033[0m'
    COLOR_E2E=$'\033[1;36m'
    COLOR_CLIENT=$'\033[1;32m'
    COLOR_SERVER=$'\033[1;35m'
    COLOR_DATA=$'\033[1;33m'
else
    COLOR_RESET=""
    COLOR_E2E=""
    COLOR_CLIENT=""
    COLOR_SERVER=""
    COLOR_DATA=""
fi

log_line() {
    local color="$1"
    local tag="$2"
    shift 2
    printf '%s[%s]%s %s\n' "${color}" "${tag}" "${COLOR_RESET}" "$*"
}

log_e2e() {
    log_line "${COLOR_E2E}" "e2e" "$@"
}

log_data() {
    log_line "${COLOR_DATA}" "data" "$@"
}

cleanup() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill -INT "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" || true
    fi
    if [[ -n "${SERVER_LOGGER_PID}" ]] && kill -0 "${SERVER_LOGGER_PID}" 2>/dev/null; then
        wait "${SERVER_LOGGER_PID}" || true
    fi
    if [[ -n "${SERVER_PIPE}" ]] && [[ -p "${SERVER_PIPE}" ]]; then
        rm -f "${SERVER_PIPE}"
    fi
}

allocate_port() {
    python3 - <<'PY'
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

wait_for_server() {
    python3 - "$1" <<'PY'
import http.client
import sys
import time

port = int(sys.argv[1])

for _ in range(50):
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        if resp.status == 200:
            sys.exit(0)
    except OSError:
        pass
    time.sleep(0.1)

print(f"server did not become ready on port {port}", file=sys.stderr)
sys.exit(1)
PY
}

run_requests() {
    python3 - "$1" <<'PY'
import http.client
import json
import os
import sys

port = int(sys.argv[1])
color_reset = os.environ.get("E2E_COLOR_RESET", "")
color_client = os.environ.get("E2E_COLOR_CLIENT", "")


def log(message):
    print(f"{color_client}[client]{color_reset} {message}", flush=True)


def request(name, method, path, payload=None, expected=200, contains=None):
    headers = {}
    body = None

    if payload is not None:
        body = json.dumps(payload)
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(body.encode("utf-8")))

    log(f"REQUEST {name}: {method} {path}")
    if body is not None:
        log(f"REQUEST BODY {name}: {body}")

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request(method, path, body=body, headers=headers)
    resp = conn.getresponse()
    response_body = resp.read().decode()
    conn.close()

    log(f"RESPONSE {name}: {resp.status} {resp.reason}")
    log(f"RESPONSE BODY {name}: {response_body}")

    if resp.status != expected:
        raise SystemExit(f"{name} expected {expected} but got {resp.status}: {response_body}")

    if contains is not None and contains not in response_body:
        raise SystemExit(f"{name} response missing expected text {contains!r}: {response_body}")

    return response_body


request("01_health", "GET", "/health", expected=200, contains='"status":"ok"')
request(
    "02_create_table",
    "POST",
    "/query",
    payload={"sql": "CREATE TABLE e2e_users (id INT, name VARCHAR)"},
    expected=200,
    contains='"status":"ok"',
)
request(
    "03_insert_alice",
    "POST",
    "/query",
    payload={"sql": 'INSERT INTO e2e_users (id, name) VALUES (1, "alice")'},
    expected=200,
    contains='"status":"ok"',
)
request(
    "04_insert_bob",
    "POST",
    "/query",
    payload={"sql": 'INSERT INTO e2e_users (id, name) VALUES (2, "bob")'},
    expected=200,
    contains='"status":"ok"',
)
request(
    "05_tables",
    "GET",
    "/tables",
    expected=200,
    contains="e2e_users",
)
request(
    "06_select_users",
    "POST",
    "/query",
    payload={"sql": "SELECT * FROM e2e_users"},
    expected=200,
    contains='"count":2',
)
request(
    "07_missing_route",
    "GET",
    "/missing",
    expected=404,
    contains='"error":"not found"',
)
request(
    "08_method_not_allowed",
    "GET",
    "/query",
    expected=405,
    contains='"error":"method not allowed"',
)
request(
    "09_invalid_sql",
    "POST",
    "/query",
    payload={"sql": "THIS IS NOT VALID SQL"},
    expected=500,
    contains="{}",
)
PY
}

stream_server_logs() {
    while IFS= read -r line; do
        log_line "${COLOR_SERVER}" "server" "${line}"
    done < "${SERVER_PIPE}"
}

print_persisted_table() {
    local schema_file="${RUN_DIR}/data/schema/e2e_users.schema"
    local table_file="${RUN_DIR}/data/tables/e2e_users.csv"

    if [[ -f "${schema_file}" ]]; then
        log_data "schema: ${schema_file}"
        while IFS= read -r line; do
            log_data "${line}"
        done < "${schema_file}"
    fi

    if [[ -f "${table_file}" ]]; then
        log_data "table: ${table_file}"
        while IFS= read -r line; do
            log_data "${line}"
        done < "${table_file}"
    fi
}

main() {
    local port

    if [[ ! -x "${SERVER_BIN}" ]]; then
        echo "missing server binary: ${SERVER_BIN}" >&2
        exit 1
    fi

    command -v python3 >/dev/null

    mkdir -p "${RUN_PARENT}"
    RUN_DIR="$(mktemp -d "${RUN_PARENT}/run.XXXXXX")"
    SERVER_PIPE="${RUN_DIR}/server.pipe"
    port="${PORT:-$(allocate_port)}"
    export E2E_COLOR_RESET="${COLOR_RESET}"
    export E2E_COLOR_CLIENT="${COLOR_CLIENT}"

    log_e2e "run directory: ${RUN_DIR}"
    log_e2e "starting server on port ${port}"

    mkfifo "${SERVER_PIPE}"
    stream_server_logs &
    SERVER_LOGGER_PID=$!

    pushd "${RUN_DIR}" >/dev/null
    "${SERVER_BIN}" "${port}" > "${SERVER_PIPE}" 2>&1 &
    SERVER_PID=$!
    popd >/dev/null

    wait_for_server "${port}"
    log_e2e "server is ready"
    run_requests "${port}"

    kill -INT "${SERVER_PID}"
    wait "${SERVER_PID}"
    SERVER_PID=""
    wait "${SERVER_LOGGER_PID}" || true
    SERVER_LOGGER_PID=""

    print_persisted_table
    log_e2e "passed"
}

trap cleanup EXIT

main "$@"
