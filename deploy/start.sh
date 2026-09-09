#!/usr/bin/env bash
# Run on the NEW VM, from inside the copied project folder.
#
#   ./deploy/start.sh                 # auto-detect IP, full setup
#   ./deploy/start.sh 10.0.1.97       # pass the IP the browser will use
#   ./deploy/start.sh --skip-images   # DB + services only, no image fetch
#
# Steps: detect IP → restore DB → re-fetch originals from OCI → redraw
# annotations from the violations table → build and start everything.
set -euo pipefail

SKIP_IMAGES=0
IP_ARG=""
for arg in "$@"; do
  case "$arg" in
    --skip-images) SKIP_IMAGES=1 ;;
    *)             IP_ARG="$arg" ;;
  esac
done

cd "$(dirname "$0")/.."

# ── Figure out the IP the browser will use ──────────────────────────────────
# Auto-detection finds the primary private address. For a cloud VM reached over
# the internet, pass the public IP as an argument instead.
IP="${IP_ARG:-$(hostname -I 2>/dev/null | awk '{print $1}')}"
[ -n "$IP" ] || { echo "ERROR: could not detect IP — pass it: ./deploy/start.sh <ip>"; exit 1; }
echo "==> Host IP: $IP"

# ── Preflight ───────────────────────────────────────────────────────────────
command -v docker >/dev/null || {
  echo "ERROR: docker not installed. Run:"
  echo "  curl -fsSL https://get.docker.com | sudo sh"
  echo "  sudo usermod -aG docker \$USER   # then log out and back in"
  exit 1
}
if docker compose version >/dev/null 2>&1; then DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then DC="docker-compose"
else echo "ERROR: install compose: sudo apt-get install -y docker-compose-plugin"; exit 1; fi

[ -f .env ] || { echo "ERROR: .env missing — the copy did not complete."; exit 1; }

CORES=$(nproc)
echo "    cores: $CORES (compose declares 73 worker replicas)"
[ "$CORES" -lt 16 ] && echo "    WARNING: lower the 'replicas:' values in docker-compose.yaml on a box this size"

# Sizing is driven by the frames table, not by what happens to sit in
# downloaded_images/ on the source box. The current DB holds 73k frames
# (Makkah 002001 + Jeddah 005001) averaging 260 KB → ~18 GB of originals, plus
# ~3 GB of redrawn annotations for the ~10k frames that have violations, plus
# docker build layers. Refuse to start a fetch that cannot finish.
AVAIL_GB=$(df -BG --output=avail . | tail -1 | tr -dc '0-9')
echo "    free disk: ${AVAIL_GB}G"
if [ "$SKIP_IMAGES" = 0 ] && [ "$AVAIL_GB" -lt 40 ]; then
  echo
  echo "ERROR: need ~40G free (18G originals + 3G annotated + build layers)."
  echo "       Free space, then re-run. Quick wins:"
  echo "         docker system prune -a --volumes"
  echo "         sudo du -xh --max-depth=1 / | sort -rh | head -20"
  echo "       Or run with --skip-images to bring up the DB and services only."
  exit 1
fi

# ── Point Streamlit's image links at this VM ────────────────────────────────
# map.html needs no change — it derives its API base from window.location.
if grep -q '^PUBLIC_HOST=' .env; then
  sed -i "s|^PUBLIC_HOST=.*|PUBLIC_HOST=$IP|" .env
else
  printf '\n# Host the browser uses to reach this VM (set by deploy/start.sh)\nPUBLIC_HOST=%s\n' "$IP" >> .env
fi
echo "    PUBLIC_HOST=$IP"

# ── Datastores ──────────────────────────────────────────────────────────────
echo
echo "==> Starting mysql + rabbitmq"
$DC up -d mysql rabbitmq

echo "==> Waiting for MySQL"
for _ in $(seq 1 60); do
  status=$(docker inspect -f '{{.State.Health.Status}}' pci_mysql 2>/dev/null || echo starting)
  [ "$status" = "healthy" ] && break
  sleep 5
done
[ "$status" = "healthy" ] || { echo "ERROR: MySQL unhealthy. Check: $DC logs mysql"; exit 1; }

# db/init.sql created an empty schema on the fresh volume; the dump carries
# DROP TABLE IF EXISTS, so restoring over it is safe.
if [ -f deploy/dump/pci_dump.sql.gz ]; then
  echo "==> Restoring database"
  gunzip -c deploy/dump/pci_dump.sql.gz | docker exec -i pci_mysql mysql -uroot -prootpassword
  docker exec pci_mysql mysql -uroot -prootpassword -e \
    "SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema='pci';"
else
  echo "==> No dump found — starting with the empty schema from db/init.sql"
fi

# ── Images ──────────────────────────────────────────────────────────────────
if [ "$SKIP_IMAGES" = 0 ]; then
  mkdir -p downloaded_images/annotated

  # Both helpers live in deploy/, which is not in the worker build context, so
  # mount it in. download_worker's image already has boto3, Pillow and
  # mysql-connector — nothing extra to install.
  echo
  echo "==> Building download_worker image (needed by the helpers)"
  $DC build download_worker

  RUN="$DC run --rm --no-deps -v $PWD/deploy:/app/deploy"

  echo
  echo "==> Re-fetching originals from OCI (resumable — re-run if interrupted)"
  $RUN download_worker python /app/deploy/fetch_images.py || {
    echo "WARNING: some downloads failed — see downloaded_images/fetch_failures.tsv"
    echo "         re-run ./deploy/start.sh to retry them"
  }

  # Annotated images come from the bucket, so there is nothing to redraw or
  # copy — /api/image redirects to a presigned URL. Only regenerate when the
  # bucket is not configured.
  if grep -qE '^(ANNOTATED_S3_BUCKET|Bucket_name)[ =]' .env; then
    echo
    echo "==> Annotated images served from object storage — skipping redraw"
  else
    echo
    echo "==> Redrawing annotated images from the violations table"
    $RUN download_worker python /app/deploy/regenerate_annotated.py || {
      echo "WARNING: some annotations failed — see downloaded_images/annotated/draw_failures.tsv"
    }
  fi
else
  echo
  echo "==> --skip-images: not fetching originals or redrawing annotations"
fi

# ── Everything else ─────────────────────────────────────────────────────────
echo
echo "==> Building and starting all services"
$DC up -d --build
$DC ps

cat <<EOF

Done.

  Map / Flask   http://$IP:5000/map
  Streamlit     http://$IP:8501
  RabbitMQ      http://$IP:15672   (admin / mypass)

Open those ports in the VM's firewall / security group to reach them from
outside.

Check the model server is reachable from here — the most common post-move
failure, since MODEL_URL points at a private address:
  curl -sS -o /dev/null -w '%{http_code}\n' \$(grep '^MODEL_URL=' .env | cut -d= -f2-)

Logs:  $DC logs -f processing_engine
EOF
