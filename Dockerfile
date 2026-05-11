# syntax=docker/dockerfile:1.7

# Stage 1: build the SPA. We stage it into internal/web/embedded/ before the
# Go compile so //go:embed all:embedded picks up the production assets.
FROM node:20-alpine AS ui-builder
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# Stage 2: build the Go binary with the SPA embedded.
FROM golang:1.25-alpine AS go-builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Replace the placeholder-only embedded/ with the freshly built dist so
# //go:embed all:embedded compiles in the real SPA.
RUN rm -rf internal/web/embedded && mkdir -p internal/web/embedded
COPY --from=ui-builder /ui/dist/ internal/web/embedded/
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w" \
        -o /out/storman ./cmd/storman

# Stage 3: alpine runtime. We need postgresql-client for `storman backup`
# (pg_dump) and `storman recover` (pg_restore) — see internal/backup. Client
# major version must match the postgres server in deployments/docker-compose.yml.
FROM alpine:3.20
RUN apk add --no-cache postgresql16-client \
    && addgroup -g 65532 -S nonroot \
    && adduser -u 65532 -S -G nonroot nonroot
COPY --from=go-builder /out/storman /usr/local/bin/storman
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/storman"]
