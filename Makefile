BIN := bin/storman
PKG := ./...

# Dev environment knobs. Override on the command line:
#   make dev DEV_DATA=/tmp/storman
DEV_DATA     ?= $(CURDIR)/dev-data
DEV_USER     ?= dev
DEV_PASSWORD ?= devdevdevdev
PG_CONTAINER ?= storman-pg

.PHONY: help build test vet tidy clean \
        run-pg stop-pg \
        dev dev-data dev-backend dev-ui dev-down dev-reset

help:
	@echo "Build & test"
	@echo "  build         build storman binary"
	@echo "  test          go test ./..."
	@echo "  vet           go vet ./..."
	@echo "  tidy          go mod tidy"
	@echo "  clean         remove ./bin"
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
build-ui: ui/node_modules
	@echo "==> building SPA"
	cd ui && npm run build
	@echo "==> staging dist into internal/web/embedded/"
	@rm -rf internal/web/embedded
	@mkdir -p internal/web/embedded
	@cp -r ui/dist/. internal/web/embedded/

# build-go-only is an alias that intentionally skips the UI build for users
# who want to embed a stale (but present) bundle.
build-go-only: build-go

test:
	go test $(PKG)

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
	  echo "==> starting $(PG_CONTAINER)"; \
	  docker run --rm -d --name $(PG_CONTAINER) \
	    -e POSTGRES_PASSWORD=storman \
	    -e POSTGRES_USER=storman \
	    -e POSTGRES_DB=storman \
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
	  $(BIN) init --data-dir=$(DEV_DATA); \
	fi
	@python3 -c "import json,pathlib; p=pathlib.Path('$(DEV_DATA)/config.json'); c=json.loads(p.read_text()); c.setdefault('web',{})['secure_cookies']=False; p.write_text(json.dumps(c, indent=2)+chr(10))"
	@$(BIN) migrate --data-dir=$(DEV_DATA)
	@$(BIN) bootstrap --data-dir=$(DEV_DATA)
	@if ! docker exec $(PG_CONTAINER) psql -U storman -d storman -tAc "SELECT 1 FROM users WHERE login='$(DEV_USER)'" 2>/dev/null | grep -q 1; then \
	  $(BIN) useradd --data-dir=$(DEV_DATA) --login=$(DEV_USER) --password=$(DEV_PASSWORD); \
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
	$(BIN) serve --data-dir=$(DEV_DATA)

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
	  $(BIN) serve --data-dir=$(DEV_DATA) & \
	  (cd ui && npm run dev) & \
	  wait

dev-down: stop-pg
	@pkill -f "$(BIN) serve" 2>/dev/null || true
	@pkill -f "node.*vite"  2>/dev/null || true
	@echo "==> dev processes stopped"

dev-reset: dev-down
	rm -rf $(DEV_DATA)
	@echo "==> wiped $(DEV_DATA)"
