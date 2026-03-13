#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Generate self-signed TLS certificate for test Postgres.
# Output files are committed to the repo for CI convenience.
# DO NOT use these certs in production.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

main() {
  cd "${SCRIPT_DIR}"

  echo "Generating self-signed certificate for test Postgres..."

  openssl req \
    -new \
    -x509 \
    -days 3650 \
    -nodes \
    -text \
    -out server.crt \
    -keyout server.key \
    -subj "/CN=localhost"

  # Postgres requires the key to be owned by the postgres user
  # and not group/world readable. Inside Docker, the postgres
  # process runs as uid 999; we set permissions here so the
  # volume mount is usable without extra entrypoint steps.
  chmod 600 server.key

  echo "Done: server.crt and server.key written to ${SCRIPT_DIR}"
}

main "$@"
