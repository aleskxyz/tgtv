# TGTV development

Run TGTV from a git checkout with a local MediaMTX instance. Production installs use `tgtv.sh` and `/opt/tgtv`; this layout is for hacking on ingest, scheduler, and API code.

## Prerequisites

| Tool | Purpose |
|------|---------|
| Go 1.25+ | Build `tgtv` |
| ffmpeg | Per-part remux and RTMP publish |
| Docker + Compose | Dev MediaMTX only (optional if you run MediaMTX elsewhere) |

## Quick start

Export Telegram API credentials from [my.telegram.org](https://my.telegram.org) (not stored in `dev/.env`):

```bash
export TELEGRAM_API_ID=12345678
export TELEGRAM_API_HASH=your_api_hash

cd tgtv
set -a
source dev/.env
set +a

make build
make mediamtx-dev

./.build/tgtv login
./.build/tgtv serve 2>&1 | tee tgtv.log
./.build/tgtv url
```

Stop MediaMTX when done: `make mediamtx-down`

## Make targets

```bash
make build          # ./.build/tgtv
make test           # go test ./...
make mediamtx-dev   # start dev MediaMTX
make mediamtx-down  # stop dev MediaMTX
```

## Log analysis

```bash
python3 dev/analyze_log.py tgtv.log
python3 dev/analyze_log.py tgtv.log --stream <stream_id>
python3 dev/analyze_log.py tgtv.log --verdict-only
python3 dev/analyze_log.py tgtv.log --json
```

## Architecture

See [`DESIGN.md`](DESIGN.md) for discovery, ingest modes (unified vs separate A/V), scheduler pacing, recovery, and HLS proxy behavior.

## Dev vs production stack

| | **Dev (this guide)** | **Production (`tgtv.sh`)** |
|--|----------------------|----------------------------|
| TGTV | Native `./.build/tgtv serve` | Docker `ghcr.io/aleskxyz/tgtv` |
| MediaMTX | `dev/docker-compose.dev.yml` | `docker-compose.yml` in install dir |
| Edge / HTTPS | Direct `PUBLIC_BASE_URL` | Caddy |
| Config | `set -a; source dev/.env; set +a` | `/opt/tgtv/.env` |

If the production Docker stack is running, stop it before binding dev ports (`8090`, `1935`, `8888`):

```bash
docker stop tgtv tgtv-caddy tgtv-mediamtx 2>/dev/null || true
```
