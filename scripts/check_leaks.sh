#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVER_BIN="${PROJECT_ROOT}/bin/dbms_server"
RUN_PARENT="${PROJECT_ROOT}/test-results/resource"
RUN_DIR=""
SERVER_PID=""

cleanup() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill -INT "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" || true
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
import sys

port = int(sys.argv[1])

def request(method, path, payload=None, expected=200):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    headers = {}
    body = None
    if payload is not None:
        body = json.dumps(payload)
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(body.encode("utf-8")))
    conn.request(method, path, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read().decode()
    conn.close()
    if resp.status != expected:
        raise SystemExit(f"{method} {path} failed: {resp.status} {data}")

request("GET", "/health")
request("POST", "/query", {"sql": "CREATE TABLE leak_users (id INT, name VARCHAR)"})
request("POST", "/query", {"sql": "INSERT INTO leak_users VALUES (1, \"alice\")"})
request("POST", "/query", {"sql": "SELECT * FROM leak_users"})
PY
}

main() {
    local port
    local valgrind_log
    local server_log

    if [[ ! -x "${SERVER_BIN}" ]]; then
        echo "missing server binary: ${SERVER_BIN}" >&2
        exit 1
    fi

    command -v valgrind >/dev/null
    command -v python3 >/dev/null

    mkdir -p "${RUN_PARENT}"
    RUN_DIR="$(mktemp -d "${RUN_PARENT}/leaks.XXXXXX")"
    port="${PORT:-$(allocate_port)}"
    valgrind_log="${RUN_DIR}/valgrind.log"
    server_log="${RUN_DIR}/server.log"

    pushd "${RUN_DIR}" >/dev/null
    valgrind \
        --leak-check=full \
        --show-leak-kinds=all \
        --track-fds=yes \
        --error-exitcode=1 \
        --log-file="${valgrind_log}" \
        "${SERVER_BIN}" "${port}" >"${server_log}" 2>&1 &
    SERVER_PID=$!
    popd >/dev/null

    wait_for_server "${port}"
    run_requests "${port}"

    kill -INT "${SERVER_PID}"
    wait "${SERVER_PID}"
    SERVER_PID=""

    grep -q "definitely lost: 0 bytes" "${valgrind_log}"
    grep -q "indirectly lost: 0 bytes" "${valgrind_log}"

    echo "check-leaks passed"
    echo "logs: ${RUN_DIR}"
}

trap cleanup EXIT

main "$@"
