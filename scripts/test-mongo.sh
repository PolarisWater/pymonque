#!/usr/bin/env bash
# Run the test suite against a real MongoDB — a single-node replica set in Docker, so majority writes
# and the server's clock behave as in production — instead of mongomock. Local only: it needs Docker.
#
#   scripts/test-mongo.sh                 # the whole suite
#   scripts/test-mongo.sh tests/test_task_claims.py -k race
#
# MONGO_IMAGE (default mongo:7) and MONGO_PORT (default 27099) choose the server. The container is
# removed afterwards, whatever the result.
set -euo pipefail

image="${MONGO_IMAGE:-mongo:7}"
port="${MONGO_PORT:-27099}"
name="pymonque-test-mongo-$$"

docker run -d --rm --name "$name" --ulimit nofile=64000:64000 -p "$port:27017" "$image" --replSet rs0 --bind_ip_all >/dev/null
trap 'docker stop "$name" >/dev/null 2>&1 || true' EXIT

for _ in $(seq 1 30); do
    if docker exec "$name" mongosh --quiet --eval \
        'try { rs.status().ok } catch (e) { rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]}).ok }' \
        >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

until docker exec "$name" mongosh --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -q true; do
    sleep 0.5
done

cd "$(dirname "$0")/.."
PYMONQUE_MONGO_URL="mongodb://localhost:$port/?directConnection=true" uv run pytest "$@"
