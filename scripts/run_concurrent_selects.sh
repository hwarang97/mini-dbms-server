#!/usr/bin/env bash

set -u

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8080}"
REQUEST_COUNT="${REQUEST_COUNT:-10}"
SQL="${SQL:-SELECT * FROM users}"
URL="http://${HOST}:${PORT}/query"
TMP_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "${TMP_DIR}"
}

trap cleanup EXIT

if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to run this script." >&2
    exit 1
fi

if ! [[ "${REQUEST_COUNT}" =~ ^[0-9]+$ ]] || [[ "${REQUEST_COUNT}" -le 0 ]]; then
    echo "REQUEST_COUNT must be a positive integer." >&2
    exit 1
fi

escaped_sql="${SQL//\\/\\\\}"
escaped_sql="${escaped_sql//\"/\\\"}"
payload=$(printf '{"sql":"%s"}' "${escaped_sql}")

echo "[demo] ${URL} 로 ${REQUEST_COUNT}개의 동시 요청을 보냅니다."

for i in $(seq 1 "${REQUEST_COUNT}"); do
    (
        body_file="${TMP_DIR}/body_${i}.txt"
        code_file="${TMP_DIR}/code_${i}.txt"
        error_file="${TMP_DIR}/error_${i}.txt"

        curl -sS -X POST "${URL}" \
            -H "Content-Type: application/json" \
            -d "${payload}" \
            -o "${body_file}" \
            -w "%{http_code}" \
            > "${code_file}" 2> "${error_file}"

        echo "$?" > "${TMP_DIR}/exit_${i}.txt"
    ) &
done

wait

success_count=0

for i in $(seq 1 "${REQUEST_COUNT}"); do
    body_file="${TMP_DIR}/body_${i}.txt"
    code_file="${TMP_DIR}/code_${i}.txt"
    error_file="${TMP_DIR}/error_${i}.txt"
    exit_file="${TMP_DIR}/exit_${i}.txt"
    curl_exit_code="$(cat "${exit_file}")"
    http_code="$(cat "${code_file}")"

    if [[ "${curl_exit_code}" == "0" ]]; then
        success_count=$((success_count + 1))
        echo "요청 ${i} 응답 코드: ${http_code}"
        cat "${body_file}"
        echo
    else
        echo "요청 ${i} 실패 (curl exit ${curl_exit_code})"
        cat "${error_file}"
        echo
    fi
done

echo "총 ${success_count}/${REQUEST_COUNT}개 요청이 응답을 반환했습니다."

if [[ "${success_count}" -ne "${REQUEST_COUNT}" ]]; then
    exit 1
fi
