#!/usr/bin/env bash
# Coverage gate for internal/storage/... Run by `make cover` and CI.
#
# Usage: scripts/check-coverage.sh <profile> <threshold-percent>
#   profile     path to a -coverprofile output (e.g. cover.out)
#   threshold   integer or decimal percent — fail if storage total drops below
#
# The script aggregates only the lines in profile whose package starts with
# `github.com/alexnav/storman/internal/storage/`. The threshold applies to
# that aggregate, not to non-storage code (which has its own dedicated
# tests but isn't part of this durability initiative's coverage gate yet).
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <profile> <threshold-percent>" >&2
    exit 2
fi

profile="$1"
threshold="$2"

if [[ ! -s "$profile" ]]; then
    echo "coverage profile missing or empty: $profile" >&2
    exit 1
fi

# Filter the profile to just storage packages, then ask `go tool cover` for
# the total over that subset. The header line ("mode: …") must be preserved.
filtered=$(mktemp)
trap 'rm -f "$filtered"' EXIT

head -1 "$profile" > "$filtered"
# Production storage code only. storagetest/ and archtest/ are test-only
# helpers and should not drag the gated metric down.
grep '^github.com/alexnav/storman/internal/storage/' "$profile" \
    | grep -v '^github.com/alexnav/storman/internal/storage/storagetest/' \
    | grep -v '^github.com/alexnav/storman/internal/storage/archtest/' \
    >> "$filtered" || true

if [[ $(wc -l < "$filtered") -le 1 ]]; then
    echo "no storage coverage lines in $profile" >&2
    exit 1
fi

total=$(go tool cover -func="$filtered" | awk '/^total:/ {gsub("%",""); print $3}')

awk -v t="$total" -v th="$threshold" 'BEGIN {
    if (t+0 < th+0) {
        printf "coverage gate FAILED: internal/storage/... = %s%% (threshold %s%%)\n", t, th
        exit 1
    }
    printf "coverage gate ok: internal/storage/... = %s%% (>= %s%%)\n", t, th
}'
