BIN := bin/storman
PKG := ./...

# Dev environment knobs. Override on the command line:
#   make dev DEV_DATA=/tmp/storman
DEV_DATA     ?= $(CURDIR)/dev-data
DEV_USER     ?= dev
DEV_PASSWORD ?= devdevdevdev
PG_CONTAINER ?= storman-pg

.PHONY: help build test vet tidy clean \
        test-storage cover stress fuzz pg-clean \
        run-pg stop-pg \
        dev dev-data dev-backend dev-ui dev-down dev-reset \
        docker-build docker-up docker-down release-tag

# Coverage gate threshold (aggregate over internal/storage/...). Starts at
# 70% which we comfortably hit today; target is 85% (covering GC scheduler
# and recover-from-disk branches). Push it up as those land.
STORAGE_COVER_MIN ?= 70
# How long to fuzz each entry point in `make fuzz`. Override for nightly.
FUZZ_TIME ?= 30s
# Stress duration. Default off; CI nightly job overrides.
STRESS_DURATION ?= 0s

help:
	@echo "Build & test"
	@echo "  build         build storman binary"
	@echo "  test          go test ./..."
	@echo "  test-storage  go test -race ./internal/storage/..."
	@echo "  cover         coverage report + gate at $(STORAGE_COVER_MIN)% for internal/storage/..."
	@echo "  stress        long-running concurrency stress (STRESS_DURATION=5m make stress)"
	@echo "  fuzz          run fuzz entry points for FUZZ_TIME each (default 30s)"
	@echo "  vet           go vet ./..."
	@echo "  tidy          go mod tidy"
	@echo "  clean         remove ./bin"
	@echo "  pg-clean      drop leaked test databases (storman_test_*)"
	@echo ""
	@echo "Dev stack (data dir: $(DEV_DATA))"
	@echo "  dev           one-shot: PG + provisioning + backend + Vite UI (HMR)"
	@echo "  dev-data      provision data dir, run migrations, create $(DEV_USER) user — idempotent"
	@echo "  dev-backend   run only 'storman serve' against the dev data dir"
	@echo "  dev-ui        run only the Vite dev server"
	@echo "  dev-down      stop backend, Vite, and Postgres"
	@echo "  dev-reset     dev-down + wipe $(DEV_DATA)"
	@echo ""
	@echo "Postgres helpers"
	@echo "  run-pg        start the throwaway postgres container (idempotent)"
	@echo "  stop-pg       remove the postgres container"
	@echo ""
	@echo "Docker / release"
	@echo "  docker-build  build the production image as storman:dev"
	@echo "  docker-up     bring up compose stack with the local storman:dev image"
	@echo "  docker-down   stop the compose stack (data is preserved)"
	@echo "  release-tag   instructions for cutting a new version (no automation)"

# ------------------------------------------------------------------------------
# Build & test
# ------------------------------------------------------------------------------

build: build-ui build-go

# build-go compiles just the binary, embedding whatever is currently sitting
# in internal/web/embedded/. Use this for quick Go-only iterations.
build-go:
	@mkdir -p bin
	go build -o $(BIN) ./cmd/storman

# build-ui runs the Vite production build and stages the result into
# internal/web/embedded/ where //go:embed picks it up. Idempotent — re-runs
# wipe the previous embedded copy first so removed assets don't linger.
# placeholder.html is the tracked stand-in needed for //go:embed to succeed
# on a fresh clone before any build has run; it's preserved across resyncs.
build-ui: ui/node_modules
	@echo "==> building SPA"
	cd ui && npm run build
	@echo "==> staging dist into internal/web/embedded/"
	@mkdir -p internal/web/embedded
	@find internal/web/embedded -mindepth 1 -not -name placeholder.html \
	  -not -path internal/web/embedded -exec rm -rf {} + 2>/dev/null || true
	@cp -r ui/dist/. internal/web/embedded/

# build-go-only is an alias that intentionally skips the UI build for users
# who want to embed a stale (but present) bundle.
build-go-only: build-go

test:
	go test $(PKG)

# test-storage targets the durability layer specifically. Always runs under
# -race because every bug we've ever seen here was a concurrency one.
test-storage:
	go test -race -count=1 ./internal/storage/...

# cover writes a coverage profile for the whole repo, then gates on the
# aggregate over internal/storage/... via scripts/check-coverage.sh.
# storagetest/ and archtest/ are test-only helpers; instrumenting them via
# -coverpkg breaks under stripped Go 1.25 toolchains (missing covdata) and
# they're filtered from the gate anyway.
cover:
	COVPKGS=$$(go list ./... | grep -vE '/internal/storage/(storagetest|archtest)$$' | paste -sd, -); \
	go test -race -count=1 -coverpkg="$$COVPKGS" -coverprofile=cover.out ./... >/dev/null
	@scripts/check-coverage.sh cover.out $(STORAGE_COVER_MIN)
	@go tool cover -func=cover.out | tail -1

# stress drives concurrent goroutines on disjoint subtrees for the configured
# duration and verifies AssertConsistent at the end. Default duration is 0
# (skip); override via the env var.
stress:
	go test -race -run=TestStress -timeout=20m ./internal/storage/dbfs/... -stress=$(STRESS_DURATION)

# fuzz runs every fuzz entry point in storage for FUZZ_TIME each. Sequential
# so output is greppable; nightly CI runs longer (FUZZ_TIME=2m).
fuzz:
	go test -run=^$$ -fuzz=FuzzCleanRel -fuzztime=$(FUZZ_TIME) ./internal/storage/flat/
	go test -run=^$$ -fuzz=FuzzSplitPath -fuzztime=$(FUZZ_TIME) ./internal/storage/dbfs/

# pg-clean drops any storman_test_* databases left behind by interrupted runs.
# Safe to run any time; databases currently in use are protected by Postgres.
pg-clean:
	@PSQL_DSN="$${TEST_POSTGRES_DSN:-postgres://storman:storman@localhost:5432/postgres?sslmode=disable}"; \
	docker exec $(PG_CONTAINER) psql "postgres://storman:storman@localhost:5432/postgres?sslmode=disable" -tAc \
	  "SELECT 'DROP DATABASE IF EXISTS \"' || datname || '\";' FROM pg_database WHERE datname LIKE 'storman_test_%'" \
	  | docker exec -i $(PG_CONTAINER) psql "postgres://storman:storman@localhost:5432/postgres?sslmode=disable" || true
	@echo "==> pg-clean done"

vet:
	go vet $(PKG)

tidy:
	go mod tidy

clean:
	rm -rf bin

# ------------------------------------------------------------------------------
# Postgres
# ------------------------------------------------------------------------------

run-pg:
	@if [ -z "$$(docker ps -q --filter name=^/$(PG_CONTAINER)$$)" ]; then \
	  echo "==> starting $(PG_CONTAINER) (data: $(DEV_DATA)/postgres)"; \
	  mkdir -p $(DEV_DATA)/postgres; \
	  docker run --rm -d --name $(PG_CONTAINER) \
	    -e POSTGRES_PASSWORD=storman \
	    -e POSTGRES_USER=storman \
	    -e POSTGRES_DB=storman \
	    -v $(DEV_DATA)/postgres:/var/lib/postgresql/data \
	    -p 5432:5432 \
	    postgres:16 >/dev/null; \
	  printf "    waiting for postgres"; \
	  until docker exec $(PG_CONTAINER) pg_isready -U storman -d storman >/dev/null 2>&1; do \
	    printf "."; sleep 0.5; \
	  done; \
	  echo " ready"; \
	else \
	  echo "==> $(PG_CONTAINER) already running"; \
	fi

stop-pg:
	@docker rm -f $(PG_CONTAINER) >/dev/null 2>&1 || true
	@echo "==> $(PG_CONTAINER) stopped"

# ------------------------------------------------------------------------------
# Dev stack
# ------------------------------------------------------------------------------

# Provision the dev data dir, run migrations, create the dev user, and grant
# it Read|Write|Remove|Admin on the root node. Idempotent — safe to re-run.
dev-data: build run-pg
	@if [ ! -f $(DEV_DATA)/config.json ]; then \
	  echo "==> initializing $(DEV_DATA)"; \
	  $(BIN) init --config=$(DEV_DATA)/config.json; \
	fi
	@python3 -c "import json,pathlib; p=pathlib.Path('$(DEV_DATA)/config.json'); c=json.loads(p.read_text()); c.setdefault('web',{})['secure_cookies']=False; p.write_text(json.dumps(c, indent=2)+chr(10))"
	@$(BIN) migrate --config=$(DEV_DATA)/config.json
	@$(BIN) bootstrap --config=$(DEV_DATA)/config.json
	@if ! docker exec $(PG_CONTAINER) psql -U storman -d storman -tAc "SELECT 1 FROM users WHERE login='$(DEV_USER)'" 2>/dev/null | grep -q 1; then \
	  $(BIN) useradd --config=$(DEV_DATA)/config.json --login=$(DEV_USER) --password=$(DEV_PASSWORD); \
	else \
	  echo "==> user '$(DEV_USER)' already exists"; \
	fi
	@docker exec $(PG_CONTAINER) psql -U storman -d storman -c \
	  "INSERT INTO permissions (node_id, user_id, actions) \
	   SELECT n.id, u.id, B'00001111'::bit(8) \
	   FROM nodes n, users u \
	   WHERE n.parent_id IS NULL AND u.login='$(DEV_USER)' \
	   ON CONFLICT (node_id, user_id) DO UPDATE SET actions = EXCLUDED.actions" >/dev/null
	@echo ""
	@echo "==> dev stack provisioned"
	@echo "    data dir : $(DEV_DATA)"
	@echo "    user     : $(DEV_USER) / $(DEV_PASSWORD)"

# Treat ui/node_modules as a real path target — Make skips it when present.
ui/node_modules: ui/package.json ui/package-lock.json
	@echo "==> installing UI dependencies"
	cd ui && npm install
	@touch ui/node_modules

dev-backend: dev-data
	$(BIN) serve --config=$(DEV_DATA)/config.json

dev-ui: ui/node_modules
	cd ui && npm run dev

# One-shot dev stack: backend + Vite in parallel, both stopped together on
# Ctrl+C via 'kill 0' on the process group.
dev: dev-data ui/node_modules
	@echo ""
	@echo "==> launching dev stack"
	@echo "    Backend API : http://localhost:8443"
	@echo "    UI (HMR)    : http://localhost:5173"
	@echo "    Login       : $(DEV_USER) / $(DEV_PASSWORD)"
	@echo ""
	@echo "    Ctrl+C to stop both processes."
	@echo ""
	@trap 'kill 0' EXIT INT TERM; \
	  $(BIN) serve --config=$(DEV_DATA)/config.json & \
	  (cd ui && npm run dev) & \
	  wait

dev-down: stop-pg
	@pkill -f "$(BIN) serve" 2>/dev/null || true
	@pkill -f "node.*vite"  2>/dev/null || true
	@echo "==> dev processes stopped"

dev-reset: dev-down
	rm -rf $(DEV_DATA)
	@echo "==> wiped $(DEV_DATA)"

# ------------------------------------------------------------------------------
# Docker / release
# ------------------------------------------------------------------------------

# docker-build builds the production image locally. Embedded SPA is rebuilt
# from scratch inside the multi-stage Dockerfile, so this does not depend on
# `make build-ui` having been run on the host.
docker-build:
	docker build -t storman:dev .

# docker-up runs the compose stack with the local storman:dev image. Requires
# deployments/.env with POSTGRES_PASSWORD set. Data lands in deployments/data.
docker-up:
	@test -f deployments/.env || { echo "missing deployments/.env — copy from deployments/.env.example"; exit 1; }
	STORMAN_IMAGE=storman:dev docker compose -f deployments/docker-compose.yml --env-file deployments/.env up -d

docker-down:
	docker compose -f deployments/docker-compose.yml down

# release-tag prints the (manual) release procedure. CI handles the build &
# publish on push of a v*.*.* tag.
release-tag:
	@echo "To cut a release:"
	@echo "  1. Update CHANGELOG / docs as needed."
	@echo "  2. git tag vX.Y.Z && git push origin vX.Y.Z"
	@echo "  3. GitHub Actions builds and pushes the image, then creates a Release."
