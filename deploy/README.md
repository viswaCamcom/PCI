# Replicating pci_backend on a new VM

Two commands.

```bash
# on THIS machine — copies ~12 MB
./deploy/copy_to_vm.sh ubuntu@<vm-ip>

# on the NEW VM
cd ~/pci_backend && ./deploy/start.sh
```

Then open **http://\<vm-ip\>:5000/map**.

## The 55 GB never moves — and only ~21 GB is rebuilt

`downloaded_images/` is not copied, because nothing in it is unique to this
machine:

| | On the VM | Why it doesn't need copying |
|---|---|---|
| Originals | ~18 GB, 73,256 files | Already in 5 OCI buckets. Every `frames` row carries `image_bucket` + `image_key`, both NOT NULL. Sampled mean object size: 260 KB. |
| `annotated/` | **0 — served from `pci-bucket`** | Uploaded once by `deploy/push_annotated_to_s3.py`; `/api/image` redirects the browser to a presigned URL. |

Note those are **smaller than the 55 GB / 438k files sitting in
`downloaded_images/` on the source box.** Both helpers are driven by the
database, not by the directory: the current `frames` table holds 73,256 rows and
only 10,202 of them have violations. The rest of the files on disk are leftovers
from earlier datasets (Riyadh, 016001 and friends — see the stray CSVs in the
project root) that no longer appear in the DB, so they are never fetched.

### Scope: Makkah and Jeddah

The database contains exactly two municipalities, and together they *are* the
whole table:

| Code | Region | Frames |
|---|---|---|
| `002001` | Makkah | 35,467 |
| `005001` | Jeddah | 37,789 |
| | **total** | **73,256** |

So no scoping is needed to get "just Makkah and Jeddah" — that is the default.
Both helpers still accept an explicit filter, which matters once other regions
are loaded:

```bash
MUNICIPALITIES=002001,005001 ./deploy/start.sh
```

## Annotated images live in object storage

`downloaded_images/annotated/` is no longer the source of truth. All 89,835
annotated JPEGs are in **`s3://pci-bucket/annotated/`** (OCI's S3-compatible
API, same Jeddah endpoint as the frame buckets).

- **`/api/image/<frame_id>`** issues a `302` to a presigned URL, so image bytes
  go straight from object storage to the browser instead of through Flask. The
  route contract is unchanged, so `map.html` needed no edits.
- **`processing_worker`** uploads each new annotation as it is drawn and then
  deletes the local copy. Set `ANNOTATED_KEEP_LOCAL=1` to keep both.
- **Fallback order** is bucket → local `annotated/` → local original → 404, so
  a machine that still has files on disk keeps working, and frames with no
  annotation still serve their original.
- Existence is cached per gunicorn worker (4 of them), TTL 1 h for hits and
  60 s for misses, so a frame annotated after a miss starts resolving without a
  restart. Measured: ~0.20 s cold, ~0.004 s cached.

Upload or re-sync the bucket with:

```bash
docker compose run --rm --no-deps -v "$PWD/deploy:/app/deploy" \
  -e PUSH_WORKERS=24 download_worker python /app/deploy/push_annotated_to_s3.py
```

It lists the bucket first and skips anything already there at the same byte
size, so re-running after an interruption is cheap. `FORCE=1` re-uploads.

### Two OCI quirks worth knowing

**boto3 ≥ 1.36 cannot write to OCI with default settings.** It adds an
`aws-chunked` content-encoding with trailing checksums, and OCI answers
`NotImplemented: AWS chunked encoding not supported`. Every client in this repo
that writes to a bucket therefore sets:

```python
Config(request_checksum_calculation="when_required",
       response_checksum_validation="when_required")
```

**Batch `delete_objects` still fails** even with that, wanting a `Content-MD5`
or `x-amz-checksum-*` header. Delete one key at a time.

### .env keys

The bucket settings were added as `Bucket_name = 'pci-bucket'`. Docker compose
tolerates the spaces and quotes, but `source .env` and python-dotenv do not, so
they are normalised to `ANNOTATED_S3_BUCKET`, `ANNOTATED_S3_ENDPOINT`,
`ANNOTATED_S3_ACCESS_KEY`, `ANNOTATED_S3_SECRET_KEY`, plus
`ANNOTATED_S3_PREFIX` (default `annotated`) and `ANNOTATED_URL_TTL` (default
3600 s). All the code still reads the old mixed-case spellings as a fallback.

## Originals are rebuilt on the VM

`start.sh` re-fetches them:

**`deploy/fetch_images.py`** pulls them straight from OCI, 32 threads in
parallel, over the VM's own link. Files already on disk are skipped, so an
interrupted run resumes for the cost of one `stat()` per frame. Verified against
the live stack: 12/12 fetched, and a re-run skipped all 12.

### Fallback: regenerating annotations locally

`deploy/regenerate_annotated.py` redraws annotations from the `violations` table
— **no model server calls, no re-inference**, since `label`, `bbox_*` and
`polygon_points` are all stored. It is only needed when the bucket is not
configured; `start.sh` skips it otherwise.

Verified: 15/15 redrawn and pixel-diffed against the images the original
pipeline produced, mean difference 0.03–0.08 / 255 (JPEG re-encode noise).
Measured 58 frames/s at 8 workers.

Its drawing logic mirrors `draw_annotations()` in `processing_worker/tasks.py` —
**keep the two in sync** if the palette or box style changes there.

### Why the originals are not staged through a bucket

They are already in one. Copying `citylens-momrah-frames*` into `pci-bucket` via
a laptop would mean 18 GB *out* and 18 GB *down*, where the VM can pull from the
source directly.

### If the VM cannot reach OCI

```bash
./deploy/copy_to_vm.sh --with-images ubuntu@<vm-ip>   # sends the full 55 GB
./deploy/start.sh --skip-images                       # don't re-fetch or redraw
```

Budget accordingly: at a measured 2.4 MB/s over VPN that is ~6.5 hours of pure
data, and considerably longer in practice — 438k files at ~125 KB each means
per-file round trips dominate. Run it inside `screen` or `tmux`.

## The IP is handled for you

- **`app/map.html`** derives its API base from `window.location.origin`. It's
  served by Flask and every call is same-origin, so it works at whatever address
  you open it — no config, no rebuild.
- **Streamlit** renders image links server-side, so it must be told.
  `start.sh` writes `PUBLIC_HOST=<ip>` into `.env`, which `docker-compose.yaml`
  reads.

If auto-detection picks the wrong interface — common on a cloud VM you reach by
public IP while `hostname -I` reports the private one — pass it explicitly:

```bash
./deploy/start.sh 203.0.113.9
```

## Why rsync and not git

`git clone` will not reproduce this project. Much of the working tree is
untracked or uncommitted — `map_match_mbs_frames.py`, `db/mbs_schema.sql`,
`app/static/mbs_road_geometry.geojson`, the modified `app/main.py` and worker
`tasks.py` — and `.env` is gitignored but holds the OCI credentials the workers
need. rsync copies the tree as it actually is.

## VM requirements

- **Disk: 40 GB+ free** — 18 GB originals, 3 GB annotated, plus docker build
  layers and headroom. `start.sh` refuses to start the fetch below this rather
  than run for a while and die full.
- **CPU: 16+ cores.** `docker-compose.yaml` declares 73 worker replicas
  (27 download + 27 model_call + 19 processing). `start.sh` warns on smaller
  boxes; lower the `replicas:` values or the workers thrash.
- **Docker Engine + compose v2:**
  ```bash
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker $USER   # log out and back in
  ```
- **Ports** 5000, 8501, 15672 open in the security group for outside access.
- **`MODEL_URL` in `.env` points at `http://10.0.1.187:8010`** — a private
  address. The VM must reach it, or the value needs updating. Most likely thing
  to break after the move; `start.sh` prints a curl check at the end.

## Smoke-testing before the long run

Both helpers take a `LIMIT` — prove credentials and bucket access in seconds
rather than discovering a problem an hour in:

```bash
docker compose run --rm --no-deps -v "$PWD/deploy:/app/deploy" \
  -e LIMIT=12 download_worker python /app/deploy/fetch_images.py
```

Other knobs: `FETCH_WORKERS` (default 32), `DRAW_WORKERS` (default 16),
`PUSH_WORKERS` (default 16), `MUNICIPALITIES=002001,005001` to scope by region,
`FORCE=1` to redo work that already exists.

Measured rates: redraw **58 frames/s** at 8 workers; upload to `pci-bucket`
**~7 MB/s** at 24 workers (≈25 min for all 89,835 annotated images).

## Verifying

```bash
docker compose ps
ls downloaded_images | wc -l                         # ≈ frame count
docker exec pci_mysql mysql -uroot -prootpassword \
  -e "SELECT COUNT(*) FROM pci.frames;"
curl -o /dev/null -w '%{http_code}\n' localhost:5000/map
```

Failures are recorded, not swallowed: `downloaded_images/fetch_failures.tsv` and
`downloaded_images/annotated/draw_failures.tsv`. Re-running `start.sh` retries
them — completed work is skipped.

## Notes

- `db/init.sql` only runs on a *fresh* `mysql_data` volume. To start the DB
  over: `docker compose down -v`, then re-run `./deploy/start.sh`.
- `fetch_images.py` deliberately bypasses `frame_queue`. Enqueueing would re-run
  the whole pipeline and overwrite the violation rows restored from the dump —
  we only want the image bytes.
- `RABBITMQ_URL` in `.env` references a host named `rabbit` that no longer
  exists. Dead config — the code reads `RABBITMQ_HOST`, defaulting to
  `rabbitmq`.
- `app/main.py` and `app/map.html` are bind-mounted; after editing either on the
  VM, `docker restart pci_app`.
- macOS ships Apple's **openrsync**, which rejects `--append-verify` and
  `--info=progress2`. `copy_to_vm.sh` detects this and adjusts. `brew install
  rsync` is worth it if you ever use `--with-images`.
