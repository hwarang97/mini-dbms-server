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

run_race_requests() {
    python3 - "$1" <<'PY'
import concurrent.futures
import http.client
import json
import sys

port = int(sys.argv[1])

def request(sql):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    body = json.dumps({"sql": sql})
    conn.request(
        "POST",
        "/query",
        body=body,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body.encode("utf-8"))),
        },
    )
    resp = conn.getresponse()
    data = resp.read().decode()
    conn.close()
    return resp.status, data

status, data = request("CREATE TABLE race_users (id INT, name VARCHAR)")
if status != 200:
    raise SystemExit(f"setup failed: {status} {data}")

payloads = []
for value in range(1, 31):
    payloads.append(f'INSERT INTO race_users VALUES ({value}, "u{value}")')
    payloads.append("SELECT * FROM race_users")

with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    results = list(executor.map(request, payloads))

failures = [(status, body) for status, body in results if status != 200]
if failures:
    raise SystemExit(f"concurrent request failed: {failures[0]}")

status, data = request("SELECT * FROM race_users")
if status != 200:
    raise SystemExit(f"final select failed: {status} {data}")
PY
}

main() {
    local port
    local helgrind_log
    local server_log

    if [[ ! -x "${SERVER_BIN}" ]]; then
        echo "missing server binary: ${SERVER_BIN}" >&2
        exit 1
    fi

    command -v valgrind >/dev/null
    command -v python3 >/dev/null

    mkdir -p "${RUN_PARENT}"
    RUN_DIR="$(mktemp -d "${RUN_PARENT}/races.XXXXXX")"
    port="${PORT:-$(allocate_port)}"
    helgrind_log="${RUN_DIR}/helgrind.log"
    server_log="${RUN_DIR}/server.log"

    pushd "${RUN_DIR}" >/dev/null
    valgrind \
        --tool=helgrind \
        --error-exitcode=1 \
        --log-file="${helgrind_log}" \
        "${SERVER_BIN}" "${port}" >"${server_log}" 2>&1 &
    SERVER_PID=$!
    popd >/dev/null

    wait_for_server "${port}"
    run_race_requests "${port}"

    kill -INT "${SERVER_PID}"
    wait "${SERVER_PID}"
    SERVER_PID=""

    grep -q "ERROR SUMMARY: 0 errors" "${helgrind_log}"

    echo "check-races passed"
    echo "logs: ${RUN_DIR}"
}

trap cleanup EXIT

main "$@"
