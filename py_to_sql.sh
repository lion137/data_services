set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <directory>"
  exit 1
fi

root="$1"

# Find all .py files and rename them to .sql
find "$root" -type f -name '*.py' -print0 |
  while IFS= read -r -d '' file; do
    new="${file%.py}.sql"
    echo "Renaming: $file -> $new"
    mv -- "$file" "$new"
  done

