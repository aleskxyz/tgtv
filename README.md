# TGTV

Watch Telegram channel and group **live broadcasts** in Jellyfin, VLC, Kodi, or any IPTV app that accepts an M3U playlist.

TGTV logs into your Telegram account, finds live streams in channels and groups you can access, ingests them on demand, and serves a protected **M3U/HLS playlist**. Nothing is ingested until someone opens a channel in the playlist.

## How it works

```text
Telegram live streams  →  TGTV (discover + ingest)  →  ffmpeg  →  MediaMTX (HLS)
                                                                    ↑
Jellyfin / VLC / Kodi  ←  Caddy (HTTPS, short URL)  ←  playlist + HLS proxy
```

1. **Discovery** — TGTV scans your Telegram dialogs for active live broadcasts and lists them in the playlist.
2. **On play** — when a viewer requests a stream, TGTV joins the Telegram call, downloads media segments, remuxes with ffmpeg, and publishes RTMP to MediaMTX.
3. **Playback** — clients read HLS from MediaMTX through TGTV’s HTTP API. The playlist URL is secret; Caddy can expose a short redirect path for TVs.

Streams stop after a configurable idle period with no viewers.

## Install (Docker)

Requires **Docker** and **Docker Compose**.

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/aleskxyz/tgtv/main/tgtv.sh)
```

Default install directory: `/opt/tgtv`.

**First run** asks for:

- Telegram `api_id` and `api_hash` from [my.telegram.org](https://my.telegram.org)
- Server IP or domain clients use to reach this host
- HTTP/HTTPS ports (HTTPS via Caddy when the address is public)
- Max concurrent Telegram ingests and idle grace time

Then it writes config, starts the stack, and walks through Telegram login (phone + code, 2FA if enabled).

**Later runs** of `./tgtv.sh` (or the curl command above) open a menu:

- Start / stop / restart the stack
- Reconfigure settings
- Rotate playlist secrets
- Login / logout Telegram
- Uninstall

Run `./tgtv.sh` from `/opt/tgtv`, or use the curl one-liner from anywhere.

## Use the playlist

After login, the installer prints a **short playlist URL**. Add it to your player:

| App | Steps |
|-----|--------|
| **Jellyfin** | Dashboard → Live TV → Add tuner → M3U → paste playlist URL |
| **VLC** | Media → Open Network Stream → paste URL |
| **Kodi** | PVR IPTV Simple Client → M3U playlist URL |

The playlist lists live Telegram broadcasts you can access. Open a channel to start ingest; other entries stay idle until played.

## Build from source

For development without the installer, see **[dev/README.md](dev/README.md)**.

```bash
git clone https://github.com/aleskxyz/tgtv.git
cd tgtv
go build -o tgtv ./cmd/tgtv
```

You also need **ffmpeg** on `PATH` and a running **MediaMTX** (or compatible RTMP/HLS server). Copy `.env.example` to `.env`, set `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and related variables, then:

```bash
./tgtv login
./tgtv serve
./tgtv url
```

Linux is recommended for production ingest. Use the Docker stack above for the simplest deployment.

## Development

See **[dev/README.md](dev/README.md)** for native builds, local MediaMTX, log analysis, and architecture notes ([dev/DESIGN.md](dev/DESIGN.md)).

## License

[AGPL-3.0](LICENSE)
