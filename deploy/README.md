# Production deployment

This Compose project is the production lifecycle owner for `relay`, `frontend`,
LCE from the tenant branch, and the production Neo4j instance. The validation VM
is a separate environment and must not share its Neo4j data volumes or password.

## First deployment

Run from the relay repository root on the production host. The deployment
script uses `deploy/.env` by default; it never falls back to the relay
local-development `.env` or the validation VM:

```bash
cp deploy/.env.example deploy/.env
chmod 600 deploy/.env
# Set POSTGRES_*, LCE_CLOUD_DATABASE_URL, LCE_TENANT_ASSERTION_SECRET,
# BETTER_AUTH_SECRET, NEO4J_PASSWORD, and all other production secrets.
# Keep NEO4J_PORT_BIND=127.0.0.1 unless a firewall rule is intentionally added.
# NEO4J_PASSWORD is used by Compose to build NEO4J_AUTH; it is intentionally
# not exported to the Neo4j container as a standalone NEO4J_PASSWORD variable.

./deploy/deploy.sh
```

The normal deployment starts `neo4j`, `lce`, `neo4j-projector`, `relay`, and
`frontend`. Neo4j data, logs, and the projector spool use persistent Compose
volumes. Never run `docker compose down -v` for a routine application update.

All containers that can access host-managed PostgreSQL or Redis share the
Compose `host.docker.internal:host-gateway` mapping. Keep this mapping on the
LCE HTTP process, projector, optional algorithm worker, relay, and frontend;
without it, Linux Docker sidecars cannot resolve the database hostname.

After Compose reports the services started, `deploy.sh` watches their container
identity, running/health state, and restart count for 15 seconds. This catches
services without a Docker healthcheck that start and immediately enter a crash
loop. Set `DEPLOY_STABILITY_WAIT_SECONDS` to a different non-negative number
only when the production rollout needs a longer observation window.

The deploy script updates repositories independently: LCE follows
`feat/multi-tenant-relay`, while relay and frontend follow `main`, unless an
explicit `DEPLOY_REF_*` or `DEPLOY_BRANCH_*` override is supplied.

Model configuration saves have a 90-second Relay deadline. Concurrent saves
return a conflict instead of queueing. Prompt-enhancement and rerank updates
do not acquire the index-reset barrier, and MCP notification SSE connections
do not hold that barrier. Embedding switches retain exclusive index protection;
when an index operation is active, saving returns a conflict without clearing
indexes or switching configuration. A lost save response is not proof that the
write failed: reload the current configuration before retrying.

## HTTP access rollout

Phase 1: the base Compose file does not inject `LCE_HTTP_ALLOWED_HOSTS` or
`LCE_HTTP_ALLOWED_ORIGINS`. Authenticated tenant-only mode logs at INFO; missing
Host/Origin configuration still warns. Filesystem restrictions, tenant
assertions, and header acceptance remain unchanged in this default rollout.
Keep LCE on the private network. Do not add `--allow-root` to remove a warning.
Do not set these new keys in the LCE container's own environment or persisted
`/data/.env` during phase 1: the cloud entry would explicitly enable them.

Phase 2 is deferred until the Host received by LCE has been confirmed. The
standard Relay endpoint sends `Host: lce:3000` and no Origin. Additional proxies
may change Host; the frontend's public domain is not necessarily the right
value. Uncomment and adjust the example allowlists in `deploy/.env` only after
this check, then explicitly include the optional overlay. From this directory:

```bash
docker compose --env-file .env -f docker-compose.yml -f docker-compose.http-access.yml config --quiet
docker compose --env-file .env -f docker-compose.yml -f docker-compose.http-access.yml up -d --no-deps --build --wait lce
```

The overlay refuses empty lists instead of guessing defaults. Verify tools/list,
an authenticated existing-index retrieval, and an isolated test upload after
activation. Health/readiness probes alone do not verify Host/Origin acceptance.
Requests without Origin remain accepted; mismatched headers return 403. No index
rebuild or data-volume removal is involved.

Keep the same explicit `-f` list on subsequent allowlisted deployments. Normal
`deploy.sh` uses only the base file and will remove overlay-provided allowlists
when it recreates LCE. To deliberately return to phase 1, recreate only LCE with
the base file; do not run `down -v` or delete indexes:

```bash
docker compose --env-file .env -f docker-compose.yml up -d --no-deps --wait lce
```

## Enabling graph algorithms

The algorithm worker is not started by the normal rollout. This is deliberate:
`codebase_graph_algorithm` requires a working Neo4j GDS capability, and a
worker that starts without GDS would repeatedly fail and cannot safely claim
jobs. The application-level default is also off so local/dev/cloud HTTP
processes never become GDS workers just because Neo4j credentials are present.

After validating the target image, GDS plugin, memory budget, and
`gds.version()` capability, set the plugin selection in `deploy/.env`:

```env
NEO4J_PLUGINS=["graph-data-science"]
```

Then enable the profile explicitly for that deployment:

```bash
DEPLOY_GRAPH_ALGORITHMS=true ./deploy/deploy.sh
```

The deploy script passes `LCE_NEO4J_ALGORITHM_WORKER_ENABLED=true` only to
the profile service. The HTTP LCE process and projector keep the application
fallback default `false`, so credentials alone never turn a normal process
into a GDS worker. For a one-off manual start, use:

```bash
docker compose --env-file deploy/.env --profile graph-algorithms up -d --build --wait neo4j-algorithm-worker
```

The worker registers a heartbeat only after the capability probe succeeds. If
it stops or becomes stale, the API remains `control_plane_only`; jobs stay
durable in PostgreSQL and are not silently reported as executed. To disable it
on the next managed rollout, run the normal command (or set
`DEPLOY_GRAPH_ALGORITHMS=false`); the script removes the old profile
container without deleting any Neo4j or PostgreSQL data volumes.

## Data ownership and recovery

PostgreSQL is authoritative. Neo4j is a derived, generation-bound projection.
A fresh production Neo4j starts empty and is populated by the projector from the
existing published roots; no source re-upload is required. If Neo4j data is lost,
restore a planned Neo4j backup or recreate the volumes and let the PostgreSQL
outbox/rebuild path repopulate it. Do not point production LCE at the validation
VM or copy a live `/data` directory.

If an existing root has no backfill task (for example, it was imported outside
this migration), enqueue a tenant/root-scoped rebuild through the LCE image;
do not mutate Neo4j or the PostgreSQL outbox tables manually:

```bash
docker compose --env-file deploy/.env run --rm lce \
  node dist/index.js cloud graph-rebuild --tenant-id <tenant-id> [--root-id <root-id>]
```

For a new production database, use new volume names created by this Compose
project. For an existing production Neo4j adoption, inspect and back up the old
volumes before changing Compose; never start two Neo4j containers against one
`/data` volume.
