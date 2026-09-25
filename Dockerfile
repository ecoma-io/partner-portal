# syntax=docker/dockerfile:1.7
#
# Partner Portal — single-binary OpenAI-compatible proxy with an embedded Vue
# dashboard and a durable SQLite ledger.
#
# Three stages, because the two things this image needs to be are in tension:
# the dashboard needs Node, the runtime must not have it. The dashboard is
# compiled by pnpm/vite into `dashboard/dist` and baked into the binary by
# `build.rs` (a table of `include_bytes!` calls), so the final image ships *no*
# asset directory, no Node runtime and no per-request filesystem read — only the
# binary, its CA bundle and the curl used by HEALTHCHECK.
#
# Build:
#   docker build -t partner-portal:test .
#   docker build --build-arg BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
#                --build-arg GIT_COMMIT="$(git rev-parse HEAD)" -t partner-portal:test .
#
# Run (config is required; there is no default config baked into the image):
#   docker run --rm -p 8080:8080 \
#     -v "$PWD/deploy/config/partner-portal.yaml:/etc/partner-portal/config.yaml:ro" \
#     -v partner-portal-data:/var/lib/partner-portal \
#     partner-portal:test
#
# Contract of the produced image:
#   * listens on 0.0.0.0:8080            (EXPOSE 8080; PARTNER_PORTAL_LISTEN
#     is pinned below — the listen address is an environment variable, not a
#     config field, so the published port and the bind agree by construction)
#   * config from /etc/partner-portal/config.yaml (override with PARTNER_PORTAL_CONFIG)
#   * data at   /var/lib/partner-portal  (declared VOLUME; holds partner-portal.db
#     plus its -wal/-shm siblings — these three files are one unit, never split
#     across volumes or hosts)
#   * runs as uid/gid 10001 (non-root, no login shell)
#   * the binary is PID 1 in exec form, so `docker stop`'s SIGTERM is delivered
#     straight to it; the drain path in src/main.rs then finalizes the metering
#     ledger before the process exits. Keep the binary at PID 1: wrapping it in a
#     shell would swallow the signal and turn a clean drain into a SIGKILL after
#     the stop grace period, losing queued usage records.

ARG NODE_IMAGE=node:24-bookworm-slim
ARG RUST_IMAGE=rust:1.96-slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim
ARG PNPM_VERSION=12.3.4

# ---------------------------------------------------------------------------
# Stage 1 — dashboard: pnpm install --frozen-lockfile, then vite build.
# ---------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS dashboard

ARG PNPM_VERSION
ENV PNPM_HOME=/pnpm
ENV PATH=${PNPM_HOME}:${PATH}

# pnpm is installed explicitly rather than through corepack: the version is then
# a build argument that matches the lockfile, and the build does not depend on
# corepack's key distribution. `store-dir` is fixed so the cache mount below has
# a stable target.
RUN npm install --global --no-fund --no-audit "pnpm@${PNPM_VERSION}" \
    && pnpm config set store-dir /pnpm/store --global

WORKDIR /src/dashboard

# Manifests first: the dependency layer is then only invalidated by a lockfile
# change, not by every source edit. pnpm-workspace.yaml is one of them, not a
# source file: it carries the `allowBuilds` list without which pnpm 12 refuses
# the install outright (ERR_PNPM_IGNORED_BUILDS), and leaving it behind here is
# exactly how a clean build fails while a developer's machine succeeds.
COPY dashboard/package.json dashboard/pnpm-lock.yaml dashboard/pnpm-workspace.yaml ./

# --frozen-lockfile is the point of this stage: an install that silently
# resolves past the committed lockfile would make the embedded dashboard
# unreproducible from the tag alone.
RUN --mount=type=cache,id=pnpm-store,target=/pnpm/store \
    pnpm install --frozen-lockfile

COPY dashboard/ ./
RUN pnpm build

# ---------------------------------------------------------------------------
# Stage 2 — rust: release binary with the dashboard embedded.
# ---------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS rust-build

# Baked into the binary by `option_env!` and served by GET /version, which is how
# a rolling update is confirmed to have rolled what was intended.
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown
ENV GIT_COMMIT=${GIT_COMMIT} \
    BUILD_TIME=${BUILD_TIME} \
    CARGO_TERM_COLOR=never \
    CARGO_NET_RETRY=5

WORKDIR /src

# `Cargo.lock` is committed (this is a binary, not a library) and `--locked`
# makes the build fail rather than re-resolve, so the image and the tag describe
# the same dependency graph.
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ ./src/
# There is deliberately no `COPY migrations/`. The schema is
# `include_str!("schema.sql")` from src/ledger/ and is applied on every startup
# as an idempotent batch, so neither the build nor the runtime reads a migration
# directory. (`migrations/` in the repo is an empty placeholder — untracked, and
# therefore absent from a fresh checkout, which is why copying it would fail CI
# while succeeding on the machine that happens to have the directory.)
# Cargo.toml declares a [[bench]] target, and cargo refuses to parse the manifest
# if the file it names is absent — even for a `--bin` build that never touches it.
COPY benches/ ./benches/

# build.rs walks `dashboard/dist` and generates the asset table. `index.html` is
# listed as a rerun trigger even though only `dist` is embedded.
COPY dashboard/index.html ./dashboard/
COPY --from=dashboard /src/dashboard/dist ./dashboard/dist

# The cargo caches are mounts, not layers: the registry and the target dir never
# end up in the image, but a rebuild after a source-only change still reuses the
# compiled dependencies.
#
# `--features hyper-rustls` is not optional here. Without it the connector is
# plain-HTTP-only, so an `https://` upstream — what `config.example.yaml`
# documents, and what every real OpenAI-compatible API is — fails as a 502 with
# no TLS handshake ever attempted. It is a Cargo feature rather than a default
# only so the `<1s` unit-test loop need not build rustls; every path that
# *ships* a binary opts in explicitly (see .github/workflows/release.yml).
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-target,target=/src/target \
    cargo build --release --locked --features hyper-rustls --bin partner-portal \
    && cp /src/target/release/partner-portal /usr/local/bin/partner-portal

# The binary must actually carry a dashboard. build.rs only *warns* when
# `dashboard/dist` is missing (the API still works standalone), which would ship
# a placeholder page to production. Fail the build instead. `grep -a` scans the
# binary as text; the check is on the entry document vite always emits.
RUN grep -aqi '<!doctype html>' /usr/local/bin/partner-portal \
    || { echo "ERROR: binary has no embedded dashboard; dashboard/dist was not built" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Stage 3 — runtime: debian slim, non-root, no build toolchain.
# ---------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS runtime

ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown
ARG VERSION=0.0.0
# Busted by CI (run id) so the apt layer below can never be served stale from
# the layer cache: the RUN line is constant, so buildkit would otherwise hit
# the cached layer forever, and a Debian advisory published after it was first
# built would ride every later build with no diff to fix it (#9). The default
# keeps local builds cacheable.
ARG CACHEBUST=1

LABEL org.opencontainers.image.title="partner-portal" \
      org.opencontainers.image.description="OpenAI-compatible reverse proxy with durable metering and an embedded dashboard" \
      org.opencontainers.image.source="https://github.com/ecoma-io/partner-portal" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.created="${BUILD_TIME}"

# ca-certificates: the proxy dials an HTTPS upstream. curl: HEALTHCHECK only —
# there is no shell-based liveness trick available in a distroless image that is
# worth losing a debuggable base for. Recommendation: apt is left usable (no
# lists removed beyond the install) so `docker run --user root` can still
# install a probe when debugging a production container.
#
# The upgrade in the same layer is what keeps the image scannable: the base
# image ships a package snapshot that ages, and the image scan (trivy,
# exit-code 1 on any advisory with a published fix) holds the runtime to zero
# of them. Upgrading from the current index at build time is how that gate
# stays green without waiting for the base image to be rebuilt.
RUN echo "cache-bust ${CACHEBUST}" \
    && apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && apt-get upgrade --yes \
    && rm -rf /var/lib/apt/lists/*

# Numeric uid/gid, fixed, so a bind mount or a volume can be pre-owned on the
# host without guessing, and so the same uid is used by every deployment.
RUN groupadd --system --gid 10001 portal \
    && useradd --system --uid 10001 --gid portal \
         --home-dir /var/lib/partner-portal --no-create-home \
         --shell /usr/sbin/nologin portal \
    && install -d --owner portal --group portal --mode 0750 /var/lib/partner-portal \
    && install -d --owner root   --group portal --mode 0750 /etc/partner-portal

COPY --from=rust-build --chown=root:root --chmod=0755 \
     /usr/local/bin/partner-portal /usr/local/bin/partner-portal

# SQLite defaults resolve relative to the working directory, so the default
# `database.path` lands on the data volume rather than in the container layer.
WORKDIR /var/lib/partner-portal

ENV PARTNER_PORTAL_CONFIG=/etc/partner-portal/config.yaml \
    PARTNER_PORTAL_LISTEN=0.0.0.0:8080 \
    PARTNER_PORTAL_HEALTH_URL=http://127.0.0.1:8080/healthz

# The ledger, its WAL and its SHM file. Declared so a plain `docker run` without
# a mount is still durable across a restart, and so the ownership seeded into a
# fresh named volume is 10001:10001.
VOLUME ["/var/lib/partner-portal"]

USER 10001:10001

EXPOSE 8080

# Liveness, not readiness: `/healthz` never consults SQLite, so a slow disk can
# never turn into a restart loop. PARTNER_PORTAL_HEALTH_URL already points where
# PARTNER_PORTAL_LISTEN binds; change the two together if the image ever stops
# listening on 8080.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS "$PARTNER_PORTAL_HEALTH_URL" >/dev/null || exit 1

# Exec form, deliberately: no shell, no PID-1 wrapper, SIGTERM straight through.
ENTRYPOINT ["/usr/local/bin/partner-portal"]
