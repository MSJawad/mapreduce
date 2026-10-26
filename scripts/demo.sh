#!/usr/bin/env bash
# Run the docker compose demo end to end: fetch the corpus, start one
# coordinator and WORKERS workers, wait for the job, then check the output
# against a sequential run of the same job.
#
#   WORKERS=4 JOB=wordcount scripts/demo.sh
#   JOB=grep PARAM='pattern=(?i)\bwhale\b' scripts/demo.sh
#   CORPUS_DIR=/path/to/texts scripts/demo.sh   # use your own *.txt files instead
set -euo pipefail

cd "$(dirname "$0")/.."
workers=${WORKERS:-4}
export JOB=${JOB:-wordcount} PARAM=${PARAM:-} REDUCE=${REDUCE:-8}
export MR_UID MR_GID
MR_UID=$(id -u)
MR_GID=$(id -g)

if [[ -n "${CORPUS_DIR:-}" ]]; then
  rm -rf data/corpus
  mkdir -p data/corpus
  cp "$CORPUS_DIR"/*.txt data/corpus/
else
  scripts/fetch-corpus.sh
fi
rm -rf data/output data/sequential.txt

cleanup() { docker compose down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "building image and starting 1 coordinator + $workers workers (job=$JOB)"
docker compose --progress quiet build
docker compose --progress quiet up --detach --scale worker="$workers"
docker compose wait coordinator >/dev/null
status=$(docker inspect --format '{{.State.ExitCode}}' "$(docker compose ps --all --quiet coordinator)")
docker compose logs --no-color --no-log-prefix coordinator | tail -n 4
if [[ "$status" != 0 ]]; then
  echo "coordinator exited with status $status" >&2
  docker compose logs --no-color worker | tail -n 20 >&2
  exit 1
fi

echo "checking the result against a sequential run"
param_args=()
if [[ -n "$PARAM" ]]; then
  param_args=(-param "$PARAM")
fi
docker compose run --rm --no-deps coordinator \
  sequential -job "$JOB" "${param_args[@]+"${param_args[@]}"}" -out /data/sequential.txt '/data/corpus/*.txt'
if ! cat data/output/part-* | LC_ALL=C sort | cmp -s - data/sequential.txt; then
  echo "distributed output differs from the sequential reference" >&2
  exit 1
fi
echo "output matches the sequential reference: $(cat data/output/part-* | wc -l | tr -d ' ') keys in data/output/"

if [[ "$JOB" == wordcount ]]; then
  echo "most frequent words:"
  cat data/output/part-* | sort -t$'\t' -k2,2nr | head -n 10 | awk -F'\t' '{ printf "  %-8s %s\n", $1, $2 }'
fi
