#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${BTFIND_TEST_DATABASE_URL:?Set a loopback MySQL admin URL for a disposable test server}"
cargo test -- --ignored --test-threads=1
