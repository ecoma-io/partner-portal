# Same-VPS deployment

Two instances of the same binary, one SQLite ledger, one edge, on one machine.

```
            clients
               |
        edge  (nginx, :8080)        <- the only address a client uses
         /            \
   portal-a          portal-b       <- the same image, two slots
   :8081             :8082             (published on 127.0.0.1 for operators)
         \            /
      portal-data (one volume, one partner-portal.db in WAL mode)
```

Why two instances of a single binary: an update has to hold without an
orchestrator. One instance cannot be replaced without a gap; two can, one at a
time, with the survivor serving. Why they share one ledger: metering split across
two databases is not metering. SQLite in WAL mode with a 5s busy timeout is what
makes two writer processes over one file a supported configuration rather than a
hopeful one (`src/ledger/mod.rs`).

## Files

| Path | What it is |
|---|---|
| `docker-compose.yml` | the stack: two slots, the edge, a stub upstream behind `--profile smoke` |
| `config/partner-portal.yaml` | the configuration both instances mount (one file, one ledger path) |
| `config/partner-portal.smoke.yaml` | the same, pointed at the stub upstream |
| `nginx/nginx.conf` | the edge: routing, streaming (no buffering), no retries |
| `nginx/upstream.d/upstream.conf` | **the traffic switch** — which slots are in rotation |
| `rolling-update.sh` | replaces one slot, with backup, preflight, drain and verification |
| `smoke-test.sh` | starts the stack and proves the accounting from three directions |
| `mock-upstream.py` | an OpenAI-shaped stub, used only by the smoke test |

## First run

```sh
# Put a real upstream key and at least one partner key in config/partner-portal.yaml.
PARTNER_PORTAL_IMAGE=ghcr.io/owner/partner-portal@sha256:... docker compose up -d --wait
curl -fsS http://127.0.0.1:8080/healthz
```

The dashboard is served by the edge at `/`; its API is under `/api/dashboard/*`
and takes the same `Authorization: Bearer <partner key>` as the proxy.

Host ports default to 8080 (edge), 8081 and 8082 (the slots) and are overridable
with `EDGE_PORT`, `PORTAL_A_PORT`, `PORTAL_B_PORT` — the ports the smoke test and
the update script use are the same variables, so a machine that already listens
on 8080 runs the whole thing anywhere.

The slot ports are published on `127.0.0.1` only. They exist for probes; they are
not a second way in past the edge.

## Update

```sh
cd deploy
./rolling-update.sh --instance a --image ghcr.io/owner/partner-portal@sha256:...
./rolling-update.sh --instance b --image ghcr.io/owner/partner-portal@sha256:...
```

One invocation replaces one slot; the peer keeps serving throughout. The phases
(the same list `.github/workflows/release.yml` documents) are:

1. **backup** — the ledger, through SQLite's online backup API, verified with
   `integrity_check` before anything else happens.
2. **preflight** — image pullable, `docker compose config` valid, the slot
   running, the peer up *and ready*, the edge answering, free disk above the
   floor. A failed preflight changes nothing.
3. **migration compatibility** — the new image is started once against a
   **scratch** database (the real volume is not mounted, so it cannot touch the
   ledger) and `GET /version` gives the schema version it would write. Older than
   the running schema aborts; newer warns that the migration is one-way.
4. **start new / 8. old drain** — the slot is taken out of rotation, the settle
   window lets in-flight requests finish, the old container gets `SIGTERM` and
   must log a completed drain before it is replaced by the new build in the same
   slot. Phases 4 and 8 trade places because a slot holds one container at a
   time: the new instance cannot start alongside the old one in the same place.
   The property the order protects is unchanged — a slot is out of rotation for
   the whole replacement, so a new instance that is not healthy never receives
   traffic.
5. **health** — `GET /healthz` on the new container.
6. **readiness** — `GET /readyz` until `ready` is true. This is the gate: it
   fails while the metering queue is full or shutdown has begun.
7. **traffic switch** — the slot goes back into the edge's rotation, which is a
   one-line edit plus `nginx -s reload` (graceful: workers finish in-flight
   responses before exiting). The reload is *asynchronous*: `nginx -s reload`
   returns once the signal is delivered, and until the workers holding the old
   file have retired, the configuration being served is still the old one. This
   is why the switch is applied — and given its settle — before anything is
   stopped, and never the other way round.
9. **verify** — real requests through the edge, checked against the ledger
   (`--verify-requests N`, default 3; each one is a billed upstream call).
10. **cleanup** — backups pruned to `--keep-backups`, rollback command printed.

`--dry-run` prints every step and changes nothing.

### Rollback

The rule is the same one the release workflow states: **an instance that is not
healthy and ready never receives traffic**, and traffic is what gets reverted, by
pointing the edge back — never by mutating a running image. A failed update
leaves the peer serving and the previous image untouched, so rolling back a slot
is running the script again with the previous reference, which phase 10 prints:

```sh
./rolling-update.sh --instance a --image <the image the slot was running>
```

An image is never mutated in place: a deployment that changes the bytes under a
running tag has no earlier state to roll back to.

### How to verify the update did not lose or duplicate usage

Two independent sources have to agree, and the smoke test checks them both:

```sh
./smoke-test.sh                      # the full three-way check, on a stub upstream
```

* the **upstream's** own request counter — was every request forwarded exactly once?
* the **ledger**, read through the dashboard API — one row per request, distinct
  request ids, all completed, the usage the upstream reported;
* the **rollup** the dashboard bills from, read as a delta — a second table and a
  second query path, so a row written but never rolled up (or rolled up twice)
  shows up even while the row query is satisfied.

The script also takes a slot out of rotation and proves the survivor carries the
traffic and meters it, then puts it back — the switch the update performs, tested
rather than assumed.

### The two-instance caveat, stated exactly

Crash recovery is **per owner**, not global. Each instance holds an advisory
`flock` on a file beside the database for its whole life, and recovery resolves a
row only when its owner is provably gone (`src/ledger/instance.rs`,
`src/ledger/recovery.rs`). A new instance therefore does not clobber the in-flight
rows of a live peer, which is what makes this topology supported.

One case is resolved by time instead: a row whose owner cannot be probed at all —
written by a build from before instance ownership existed, or by an owner on a
platform without `flock` — is treated as possibly-alive and only recovered once it
is older than `DEFAULT_UNKNOWN_OWNER_GRACE` (30s). The practical consequence is
limited to the **first** rollout from such a build: a streaming request that has
been running longer than 30 seconds on the old-build peer can be marked
`interrupted` while it is still alive. Every later rollout is exact, because both
sides of it record ownership.

### Other operational notes

* The ledger is three files — `.db`, `.db-wal`, `.db-shm` — and they are one
  unit. Never bind the data volume to a network filesystem: SQLite's locking does
  not survive NFS, and the failure mode is a corrupted ledger rather than an
  error.
* `stop_grace_period` is 30s and the update stops containers with `-t 60`,
  because the drain of the metering queue must finish before the process is
  killed. The application's own shutdown grace (5s) is the wait for the edge to
  stop routing, not the drain budget.
* The edge does not retry (`proxy_next_upstream off`) and does not buffer
  (`proxy_buffering off`). A retry against a metered upstream is a second billed
  request; a buffered stream is a broken dashboard.
* Order is load-bearing in a switch: take the slot out of rotation, let the
  reload land, *then* stop it. A stopped container's address is a black hole
  rather than a closed port — Docker removes its network endpoint, so a
  connection to it hangs in `connect` and ends as a 504, and with no retry that
  is a lost request. The `--settle` window (default 5s) is what covers the
  reload's latency; `deploy/smoke-test.sh` asserts the same property by polling
  until the edge's pre-switch workers have retired rather than by sleeping.
* `backups/` fills up at one file per update and is pruned to the last
  `--keep-backups` (5) by the script. The database files are gitignored.
* What this does not do: TLS termination, multi-host, or any coordination beyond
  one host's filesystem. It is a same-VPS topology by construction — the advisory
  lock and the shared volume both assume a local filesystem.
