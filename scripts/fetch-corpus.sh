#!/usr/bin/env bash
# Download a handful of public-domain books from Project Gutenberg into
# data/corpus (about 6 MB). Files already present are kept.
set -euo pipefail

cd "$(dirname "$0")/.."
dest=data/corpus
mkdir -p "$dest"

# Pride and Prejudice, Moby Dick, Frankenstein, Alice in Wonderland,
# The Adventures of Sherlock Holmes, A Tale of Two Cities,
# The Adventures of Tom Sawyer, Dracula.
books=(1342 2701 84 11 1661 98 74 345)

for id in "${books[@]}"; do
  out="$dest/pg$id.txt"
  if [[ -s "$out" ]]; then
    continue
  fi
  echo "fetching Project Gutenberg #$id"
  curl --fail --silent --show-error --location --retry 3 --retry-all-errors \
    --user-agent "mapreduce-demo (github.com/MSJawad/mapreduce)" \
    --output "$out.part" "https://www.gutenberg.org/cache/epub/$id/pg$id.txt"
  mv "$out.part" "$out"
  sleep 1 # be polite to gutenberg.org
done

echo "corpus ready: $(ls "$dest" | wc -l | tr -d ' ') books, $(du -sh "$dest" | cut -f1)"
