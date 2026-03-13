#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Generate self-signed TLS certificate for test Postgres.
# Certs are generated at test time and are NOT committed to the repo.
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
    -subj "/CN=localhost" \
    -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"

  # Postgres requires the key to be owned by the postgres user
  # and not group/world readable. Inside Docker, the postgres
  # process runs as uid 999; we set permissions here so the
  # volume mount is usable without extra entrypoint steps.
  chmod 600 server.key

  echo "Done: server.crt and server.key written to ${SCRIPT_DIR}"
}

main "$@"
