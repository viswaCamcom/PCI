#!/usr/bin/env bash
# Run on THIS machine. Copies code, .env, config and a fresh MySQL dump to the
# new VM — about 12 MB.
#
#   ./deploy/copy_to_vm.sh ubuntu@10.0.1.97
#   ./deploy/copy_to_vm.sh --with-images ubuntu@10.0.1.97
#
# downloaded_images/ is NOT copied by default, and does not need to be:
#   * the ~42 GB of originals live in OCI object storage, and every frames row
#     carries image_bucket + image_key — deploy/fetch_images.py re-pulls them on
#     the VM, in parallel, over the VM's own link
#   * the 13 GB of annotated/ are derived — deploy/regenerate_annotated.py
#     redraws them from the violations table, no model calls needed
# start.sh runs both. Pass --with-images only if the VM cannot reach OCI.
set -euo pipefail

WITH_IMAGES=0
if [ "${1:-}" = "--with-images" ]; then
  WITH_IMAGES=1
  shift
fi

REMOTE="${1:?usage: $0 [--with-images] user@vm-host [remote-path]}"
RPATH="${2:-/home/$(echo "$REMOTE" | cut -d@ -f1)/pci_backend}"

cd "$(dirname "$0")/.."

echo "==> Dumping MySQL"
mkdir -p deploy/dump
docker exec pci_mysql mysqldump -uroot -prootpassword \
  --databases pci --single-transaction --quick --routines --triggers --events \
  --default-character-set=utf8mb4 2>/dev/null | gzip > deploy/dump/pci_dump.sql.gz
echo "    $(du -h deploy/dump/pci_dump.sql.gz | cut -f1)"

echo
echo "==> Copying to $REMOTE:$RPATH"
ssh "$REMOTE" "mkdir -p '$RPATH'"

# macOS ships Apple's openrsync (advertises "2.6.9 compatible"), which lacks
# --append-verify and --info=progress2. Prefer a real rsync 3.x if installed.
RSYNC=rsync
for candidate in /opt/homebrew/bin/rsync /usr/local/bin/rsync; do
  [ -x "$candidate" ] && { RSYNC="$candidate"; break; }
done

EXTRA=()
if "$RSYNC" --version 2>/dev/null | head -1 | grep -qv openrsync &&
   "$RSYNC" --version 2>/dev/null | grep -qE 'version 3\.'; then
  echo "    using $RSYNC (3.x)"
  [ "$WITH_IMAGES" = 1 ] && EXTRA=(--append-verify --info=progress2)
else
  echo "    using $RSYNC (openrsync)"
  [ "$WITH_IMAGES" = 1 ] && EXTRA=(--stats)
fi

IMAGE_EXCLUDE=(--exclude 'downloaded_images/')
if [ "$WITH_IMAGES" = 1 ]; then
  IMAGE_EXCLUDE=()
  echo
  echo "    --with-images: sending ~55 GB / 438k files."
  echo "    Run inside screen or tmux. Ctrl-C and re-run resumes."
fi

# --partial keeps interrupted files so re-running resumes.
# No -z: the bulk is JPEGs, already compressed, so it only burns CPU.
"$RSYNC" -av --partial "${EXTRA[@]}" \
  "${IMAGE_EXCLUDE[@]}" \
  --exclude '.git/' \
  --exclude '__pycache__/' \
  --exclude '.DS_Store' \
  --exclude '*.log' \
  ./ "$REMOTE:$RPATH/"

echo
echo "==> Copied. Now on the VM:"
echo "    cd $RPATH && ./deploy/start.sh"
[ "$WITH_IMAGES" = 0 ] && \
  echo "    (start.sh will re-fetch the originals from OCI and redraw annotations)"
exit 0
