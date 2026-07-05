#!/usr/bin/env bash
# Standalone installer for the TGTV Docker stack.
# Installs to /opt/tgtv: .env, mediamtx.yml, docker compose files, Caddy config.
#
# Run from anywhere:
#   curl -fsSL https://raw.githubusercontent.com/aleskxyz/tgtv/main/tgtv.sh | bash
#
# Or from a git checkout:
#   ./tgtv.sh

set -euo pipefail

INSTALL_DIR="${TGTV_INSTALL_DIR:-/opt/tgtv}"
# Stack image versions (bump when upgrading)
TGTV_IMAGE="${TGTV_IMAGE:-ghcr.io/aleskxyz/tgtv:0.1.0}"
MEDIAMTX_IMAGE="${MEDIAMTX_IMAGE:-bluenviron/mediamtx:1.19.2}"
CADDY_IMAGE="${CADDY_IMAGE:-caddy:2.11.4-alpine}"
ENV_FILE="$INSTALL_DIR/.env"
MEDIAMTX_FILE="$INSTALL_DIR/mediamtx.yml"

for arg in "$@"; do
	case "$arg" in
		-h|--help)
			cat <<EOF
Usage: tgtv.sh

Installs TGTV to ${INSTALL_DIR}.

First run: interactive configuration, then starts the stack.
Later runs: menu — start/stop/restart (when logged in), reconfigure,
  rotate secrets, login/logout Telegram, uninstall.

Standalone:
  curl -fsSL https://raw.githubusercontent.com/aleskxyz/tgtv/main/tgtv.sh | bash

Environment overrides:
  TGTV_INSTALL_DIR   Install path (default: /opt/tgtv)
  TGTV_IMAGE         TGTV container image (default: ghcr.io/aleskxyz/tgtv:0.1.0)
  MEDIAMTX_IMAGE     MediaMTX container image
  CADDY_IMAGE        Caddy container image

Private IP/hostname → HTTP. Public domain or IP → HTTPS (Caddy automatic TLS).
EOF
			exit 0
			;;
		*) echo "Unknown option: $arg" >&2; exit 1 ;;
	esac
done

random_secret() {
	openssl rand -base64 32 | tr -d '/+=' | head -c 43
}

random_short_path() {
	printf '/%s' "$(openssl rand -base64 24 | tr -dc 'A-Za-z' | head -c 8)"
}

prompt() {
	local var_name="$1"
	local question="$2"
	local default="${3:-}"
	local value=""
	if [[ -n "$default" ]]; then
		read -r -p "$question [$default]: " value
		value="${value:-$default}"
	else
		while [[ -z "$value" ]]; do
			read -r -p "$question: " value
			[[ -n "$value" ]] || echo "  (required)" >&2
		done
	fi
	printf -v "$var_name" '%s' "$value"
}

prompt_yn() {
	local question="$1"
	local default="${2:-y}"
	local value=""
	local hint="Y/n"
	[[ "$default" =~ ^[Nn] ]] && hint="y/N"
	read -r -p "$question [$hint]: " value
	value="${value:-$default}"
	[[ "$value" =~ ^[Yy] ]]
}

load_existing() {
	[[ -f "$ENV_FILE" ]] || return 0
	# shellcheck disable=SC1090
	set -a
	source "$ENV_FILE" 2>/dev/null || true
	set +a
}

url_host() {
	echo "${1:-}" | sed -E 's#^https?://([^/:]+).*#\1#'
}

is_private_ipv4() {
	local ip="$1"
	[[ "$ip" =~ ^127\. ]] && return 0
	[[ "$ip" =~ ^10\. ]] && return 0
	[[ "$ip" =~ ^192\.168\. ]] && return 0
	[[ "$ip" =~ ^169\.254\. ]] && return 0
	if [[ "$ip" =~ ^172\.([0-9]+)\. ]]; then
		local second="${BASH_REMATCH[1]}"
		((second >= 16 && second <= 31)) && return 0
	fi
	return 1
}

is_private_host() {
	local host="${1,,}"
	host="${host%%:*}"

	[[ "$host" == "localhost" ]] && return 0
	[[ "$host" == *.local ]] && return 0
	[[ "$host" == *.lan ]] && return 0

	if [[ "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
		is_private_ipv4 "$host"
		return
	fi

	if [[ "$host" == *:* ]]; then
		[[ "$host" == ::1* || "$host" == fe80:* || "$host" == fc* || "$host" == fd* ]] && return 0
		return 1
	fi

	return 1
}

normalize_path() {
	local p="$1"
	p="${p#/}"
	echo "/${p}"
}

detect_lan_ip() {
	local ip=""
	ip="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") print $(i+1); exit}')"
	if [[ -n "$ip" ]]; then
		echo "$ip"
		return
	fi
	ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
	if [[ -n "$ip" ]]; then
		echo "$ip"
	fi
}

public_url() {
	local scheme="$1" host="$2" port="$3"
	if [[ "$scheme" == "https" && "$port" == "443" ]]; then
		echo "https://${host}"
	elif [[ "$scheme" == "http" && "$port" == "80" ]]; then
		echo "http://${host}"
	else
		echo "${scheme}://${host}:${port}"
	fi
}

ensure_install_dir() {
	if [[ ! -d "$INSTALL_DIR" ]]; then
		echo "Creating ${INSTALL_DIR}..."
		if [[ "$(id -u)" -eq 0 ]]; then
			mkdir -p "$INSTALL_DIR"/{session,config,caddy/data,caddy/config}
		else
			sudo mkdir -p "$INSTALL_DIR"/{session,config,caddy/data,caddy/config}
			sudo chown -R "$(id -u):$(id -g)" "$INSTALL_DIR"
		fi
	else
		mkdir -p "$INSTALL_DIR"/{session,config,caddy/data,caddy/config}
	fi
}

is_ip_address() {
	local host="${1%%:*}"
	[[ "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] && return 0
	[[ "$1" == *:* ]] && return 0
	return 1
}

caddy_site_routes() {
	cat <<'EOF'
	handle {$SHORT_PLAYLIST_PATH} {
		redir * /p/{$PATH_SECRET}/playlist.m3u 302
	}

	handle /p/{$PATH_SECRET}/* {
		reverse_proxy tgtv:{$TGTV_HTTP_PORT:8090} {
			flush_interval -1
			transport http {
				read_timeout 2m
				write_timeout 2m
			}
		}
	}

	handle {
		respond 404
	}
EOF
}

write_caddyfile() {
	local f="$INSTALL_DIR/caddy/Caddyfile"

	if [[ "$USE_TLS" != true ]]; then
		{
			echo ':80 {'
			caddy_site_routes
			echo '}'
		} >"$f"
	elif is_ip_address "$SERVER_HOST"; then
		{
			cat <<EOF
{
	default_sni ${SERVER_HOST}
}

${SERVER_HOST} {
	tls {
		issuer acme {
			profile shortlived
			disable_tlsalpn_challenge
		}
	}
EOF
			caddy_site_routes
			echo '}'
		} >"$f"
	else
		{
			echo '{$CADDY_DOMAIN} {'
			caddy_site_routes
			echo '}'
		} >"$f"
	fi
}

write_stack_files() {
	mkdir -p "$INSTALL_DIR/caddy/data" "$INSTALL_DIR/caddy/config"

	cat >"$INSTALL_DIR/docker-compose.yml" <<EOF
# TGTV stack — installed by tgtv.sh into /opt/tgtv
#
#   cd /opt/tgtv && docker compose up -d
#   docker compose run --rm -it tgtv tgtv login

services:
  mediamtx:
    image: ${MEDIAMTX_IMAGE}
    container_name: tgtv-mediamtx
    restart: unless-stopped
    env_file:
      - .env
    environment:
      MTX_HLSCDNSECRET: \${MEDIAMTX_HLS_CDN_SECRET}
      MTX_LOGLEVEL: \${MEDIAMTX_LOG_LEVEL}
    volumes:
      - ./mediamtx.yml:/mediamtx.yml:ro
    command: /mediamtx.yml
    expose:
      - "1935"
      - "8888"

  tgtv:
    image: ${TGTV_IMAGE}
    container_name: tgtv
    restart: unless-stopped
    env_file:
      - .env
    depends_on:
      - mediamtx
    volumes:
      - ./session:/data/session
      - ./config:/data/config
    expose:
      - "\${HTTP_PORT}"

  caddy:
    image: ${CADDY_IMAGE}
    container_name: tgtv-caddy
    restart: unless-stopped
    env_file:
      - .env
    environment:
      CADDY_DOMAIN: \${CADDY_DOMAIN}
      PATH_SECRET: \${PATH_SECRET}
      SHORT_PLAYLIST_PATH: \${SHORT_PLAYLIST_PATH}
      TGTV_HTTP_PORT: \${HTTP_PORT}
    depends_on:
      - tgtv
    ports:
      - "\${CADDY_HTTP_PORT}:80"
      - "\${CADDY_HTTPS_PORT}:443"
    volumes:
      - ./caddy/Caddyfile:/etc/caddy/Caddyfile:ro
      - ./caddy/data:/data
      - ./caddy/config:/config
EOF

	write_caddyfile

	cat >"$MEDIAMTX_FILE" <<EOF
# MediaMTX config for tgtv (written by tgtv.sh)
# mpegts HLS avoids LL-HLS gap.mp4 placeholders that VLC cannot play.

logLevel: info

hlsCDNSecret: ${MEDIAMTX_HLS_CDN_SECRET}

hls: yes
hlsAddress: :8888
hlsVariant: mpegts
hlsSegmentCount: 45
hlsSegmentDuration: 1s
hlsAlwaysRemux: yes

rtmp: yes
rtmpAddress: :1935

readTimeout: 3600s

paths:
  all_others:
EOF
}

dc() {
	docker compose --project-directory "$INSTALL_DIR" -f "$INSTALL_DIR/docker-compose.yml" "$@"
}

have_docker() {
	command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1
}

session_file_path() {
	echo "$INSTALL_DIR/session/${SESSION_NAME:-tgtv}.json"
}

check_telegram_session() {
	TELEGRAM_SESSION_STATE=""
	TELEGRAM_SESSION_DETAIL=""

	if [[ -f "$(session_file_path)" ]] && ! have_docker; then
		TELEGRAM_SESSION_STATE="unverified"
		TELEGRAM_SESSION_DETAIL="session file present (install Docker to verify login)"
		return
	fi

	if ! have_docker; then
		TELEGRAM_SESSION_STATE="missing"
		TELEGRAM_SESSION_DETAIL="no session file and Docker not available"
		return
	fi

	local out="" rc=0
	if dc ps --status running --services tgtv -q 2>/dev/null | grep -q .; then
		out="$(dc exec -T tgtv tgtv status 2>&1)" || rc=$?
	else
		out="$(dc run --rm --no-deps tgtv tgtv status 2>&1)" || rc=$?
	fi

	TELEGRAM_SESSION_DETAIL="$(echo "$out" | grep '^telegram_session:' 2>/dev/null | head -1 | sed 's/^telegram_session:[[:space:]]*//' || true)"

	if [[ $rc -eq 0 ]]; then
		TELEGRAM_SESSION_STATE="active"
		if [[ -z "$TELEGRAM_SESSION_DETAIL" ]]; then
			TELEGRAM_SESSION_DETAIL="authorized"
		fi
		return
	fi

	if echo "$out" | grep -q 'telegram_session: missing'; then
		TELEGRAM_SESSION_STATE="missing"
		TELEGRAM_SESSION_DETAIL="not logged in"
	elif echo "$out" | grep -q 'telegram_session: expired'; then
		TELEGRAM_SESSION_STATE="expired"
		TELEGRAM_SESSION_DETAIL="session expired — login required"
	else
		TELEGRAM_SESSION_STATE="error"
		TELEGRAM_SESSION_DETAIL="${out:-status check failed}"
	fi
}

telegram_logged_in() {
	[[ "${TELEGRAM_SESSION_STATE:-}" == "active" ]]
}

run_logout() {
	if [[ -f "$INSTALL_DIR/docker-compose.yml" ]]; then
		stop_stack
	fi
	dc run --rm --no-deps tgtv tgtv logout
}

run_login() {
	echo
	echo "Telegram login (check your Telegram app for the code)..."
	dc run --rm -it --no-deps tgtv tgtv login
}

start_stack() {
	echo
	echo "Starting containers..."
	dc up -d
}

finish_telegram_login() {
	check_telegram_session
	if telegram_logged_in; then
		echo "Telegram: logged in (${TELEGRAM_SESSION_DETAIL})"
		echo
		echo "Playlist URL:"
		print_playlist_url
		if ! stack_running; then
			start_stack
		fi
	elif [[ "$TELEGRAM_SESSION_STATE" == "missing" || "$TELEGRAM_SESSION_STATE" == "expired" ]]; then
		echo
		echo "Telegram login still required:"
		echo "  cd ${INSTALL_DIR} && docker compose run --rm -it tgtv tgtv login"
	fi
}

run_login_if_needed() {
	check_telegram_session

	if telegram_logged_in; then
		echo "Telegram: logged in (${TELEGRAM_SESSION_DETAIL})"
		if prompt_yn "Logout and log in again?" n; then
			run_logout
			run_login
			finish_telegram_login
		fi
	else
		run_login
		finish_telegram_login
	fi
}

print_playlist_url() {
	dc run --rm --no-deps tgtv tgtv url 2>/dev/null || true
}

stop_stack() {
	echo
	echo "Stopping containers..."
	dc down
}

stack_running() {
	[[ -n "$(dc ps --status running -q 2>/dev/null || true)" ]]
}

restart_stack() {
	echo
	echo "Restarting containers..."
	dc down
	dc up -d
}

run_uninstall() {
	echo
	echo "Uninstall removes ${INSTALL_DIR} (config, Telegram session, Caddy certs, containers)."
	if ! prompt_yn "Uninstall?" n; then
		echo "Cancelled."
		return 1
	fi

	if [[ -z "$INSTALL_DIR" || "$INSTALL_DIR" == "/" ]]; then
		echo "Refusing to remove unsafe path: ${INSTALL_DIR}" >&2
		return 1
	fi

	if have_docker && [[ -f "$INSTALL_DIR/docker-compose.yml" ]]; then
		echo "Stopping containers..."
		dc down 2>/dev/null || true
	fi

	echo "Removing ${INSTALL_DIR}..."
	if [[ "$(id -u)" -eq 0 ]]; then
		rm -rf "$INSTALL_DIR"
	else
		sudo rm -rf "$INSTALL_DIR"
	fi

	echo "Uninstalled."
	return 0
}

run_rotate_secrets() {
	load_existing
	[[ -f "$ENV_FILE" ]] || {
		echo "Not configured." >&2
		return 1
	}

	echo
	echo "Rotates path secret and short playlist URL."
	echo "Update playlist URL in Jellyfin, VLC, Kodi, etc."
	if ! prompt_yn "Rotate secrets?" n; then
		echo "Cancelled."
		return 1
	fi

	PATH_SECRET="$(random_secret)"
	SHORT_PLAYLIST_PATH="$(normalize_path "$(random_short_path)")"

	infer_tls_from_env

	write_stack_files

	sed -i "s|^PATH_SECRET=.*|PATH_SECRET=${PATH_SECRET}|" "$ENV_FILE"
	sed -i "s|^SHORT_PLAYLIST_PATH=.*|SHORT_PLAYLIST_PATH=${SHORT_PLAYLIST_PATH}|" "$ENV_FILE"

	echo
	echo "Secrets rotated."
	print_summary

	if have_docker && [[ -f "$INSTALL_DIR/docker-compose.yml" ]]; then
		check_telegram_session
		if telegram_logged_in; then
			start_stack
		fi
	fi
}

infer_tls_from_env() {
	USE_TLS=false
	SERVER_HOST="$(url_host "${PUBLIC_BASE_URL:-}")"
	if [[ "${CADDY_DOMAIN:-}" != ":80" && "${PUBLIC_BASE_URL:-}" =~ ^https:// ]]; then
		USE_TLS=true
	fi
}

print_summary() {
	infer_tls_from_env
	local lan_ip="${LAN_IP:-$(detect_lan_ip)}"
	echo
	echo "=== Done ==="
	echo "Installed to: ${INSTALL_DIR}"
	echo "  .env  mediamtx.yml  docker-compose.yml  caddy/  session/  config/"
	echo
	echo "Playlist URLs:"
	echo "  Short: ${PUBLIC_BASE_URL}${SHORT_PLAYLIST_PATH}"
	echo "  Full:  ${PUBLIC_BASE_URL}/p/${PATH_SECRET}/playlist.m3u"

	local playlist_url="${PUBLIC_BASE_URL}${SHORT_PLAYLIST_PATH}"

	echo
	echo "Jellyfin:"
	echo "  Live TV → Add tuner → M3U → ${playlist_url}"

	echo
	echo "VLC:"
	echo "  Media → Open Network Stream → paste:"
	echo "  ${playlist_url}"

	echo
	echo "Kodi:"
	echo "  Install PVR IPTV Simple Client → Configure → M3U playlist URL:"
	echo "  ${playlist_url}"

	if is_private_host "$SERVER_HOST" && [[ "$SERVER_HOST" == "127.0.0.1" || "$SERVER_HOST" == "localhost" ]] && [[ -n "$lan_ip" ]]; then
		LAN_URL="$(public_url http "$lan_ip" "$CADDY_HTTP_PORT")"
		echo "  Jellyfin in Docker on this host (use LAN IP): ${LAN_URL}${SHORT_PLAYLIST_PATH}"
	fi
	if [[ "$USE_TLS" == true ]] && ! is_ip_address "$SERVER_HOST"; then
		echo "  Point DNS for ${SERVER_HOST} to this server."
	fi
}

run_configure() {
	load_existing

	LAN_IP="$(detect_lan_ip)"
	DEFAULT_SERVER_HOST="$(url_host "${PUBLIC_BASE_URL:-}")"
	if [[ -z "$DEFAULT_SERVER_HOST" ]]; then
		DEFAULT_SERVER_HOST="${LAN_IP:-127.0.0.1}"
	fi

	echo
	echo "--- Telegram API credentials ---"
	echo "Get api_id and api_hash from Telegram (one-time, for your own account):"
	echo "  1. Open https://my.telegram.org and sign in with your phone number"
	echo "  2. Go to API development tools"
	echo "  3. Create an application (App title and Short name can be anything)"
	echo "  4. Copy api_id and api_hash — keep api_hash private"
	echo
	prompt TELEGRAM_API_ID "Telegram API ID (api_id)" "${TELEGRAM_API_ID:-}"
	prompt TELEGRAM_API_HASH "Telegram API hash (api_hash)" "${TELEGRAM_API_HASH:-}"

	echo
	echo "--- Server address ---"
	prompt SERVER_HOST "IP or domain clients use to reach this server" "$DEFAULT_SERVER_HOST"

	CADDY_HTTPS_PORT="${CADDY_HTTPS_PORT:-443}"
	USE_TLS=false

	if is_private_host "$SERVER_HOST"; then
		echo "Private address — HTTP only (no TLS)."
		CADDY_DOMAIN=":80"
		prompt CADDY_HTTP_PORT "HTTP port" "${CADDY_HTTP_PORT:-80}"
		PUBLIC_BASE_URL="$(public_url http "$SERVER_HOST" "$CADDY_HTTP_PORT")"
	else
		echo "Public address."
		TLS_DEFAULT="y"
		if [[ "${PUBLIC_BASE_URL:-}" =~ ^http:// ]]; then
			TLS_DEFAULT="n"
		fi
		if prompt_yn "Enable HTTPS with automatic TLS (Caddy)?" "$TLS_DEFAULT"; then
			USE_TLS=true
			CADDY_DOMAIN="$SERVER_HOST"
			prompt CADDY_HTTP_PORT "HTTP port" "${CADDY_HTTP_PORT:-80}"
			prompt CADDY_HTTPS_PORT "HTTPS port" "${CADDY_HTTPS_PORT:-443}"
			PUBLIC_BASE_URL="$(public_url https "$SERVER_HOST" "$CADDY_HTTPS_PORT")"
		else
			CADDY_DOMAIN=":80"
			prompt CADDY_HTTP_PORT "HTTP port" "${CADDY_HTTP_PORT:-80}"
			PUBLIC_BASE_URL="$(public_url http "$SERVER_HOST" "$CADDY_HTTP_PORT")"
		fi
	fi

	if [[ -z "${IDLE_GRACE_SECONDS:-}" ]]; then
		IDLE_GRACE_SECONDS=60
	fi

	echo
	echo "--- Live streams ---"
	echo "How many Telegram live broadcasts TGTV may ingest at the same time."
	echo "This is not a limit on HLS viewers or playlist channels."
	prompt MAX_CONCURRENT_INGESTS "Max concurrent Telegram live ingests" "${MAX_CONCURRENT_INGESTS:-10}"

	if [[ -z "${SHORT_PLAYLIST_PATH:-}" ]]; then
		SHORT_PLAYLIST_PATH="$(random_short_path)"
	fi
	SHORT_PLAYLIST_PATH="$(normalize_path "$SHORT_PLAYLIST_PATH")"

	echo
	echo "--- Ingest ---"
	prompt IDLE_GRACE_SECONDS "Stop ingest after N seconds with no viewers" "${IDLE_GRACE_SECONDS:-60}"
	LOG_LEVEL="${LOG_LEVEL:-info}"
	LOG_FORMAT="${LOG_FORMAT:-jsonl}"
	TELEGRAM_LOG_LEVEL="${TELEGRAM_LOG_LEVEL:-warn}"

	if [[ -z "${PATH_SECRET:-}" ]]; then
		PATH_SECRET="$(random_secret)"
	fi
	if [[ -z "${MEDIAMTX_HLS_CDN_SECRET:-}" ]]; then
		MEDIAMTX_HLS_CDN_SECRET="$(random_secret)"
	fi

	write_stack_files

	cat >"$ENV_FILE" <<EOF
# Generated by tgtv.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)
# Install: ${INSTALL_DIR} — re-run tgtv.sh to reconfigure.

# --- Telegram (required) ---
TELEGRAM_API_ID=${TELEGRAM_API_ID}
TELEGRAM_API_HASH=${TELEGRAM_API_HASH}

# --- Paths (host: ./session and ./config under install dir) ---
SESSION_DIR=/data/session
CONFIG_DIR=/data/config
SESSION_NAME=tgtv

# --- HTTP API (tgtv listens inside the Docker network) ---
HTTP_HOST=0.0.0.0
HTTP_PORT=8090
PATH_SECRET=${PATH_SECRET}

# --- Client-facing URL (must match what browsers/TVs use to reach Caddy) ---
PUBLIC_BASE_URL=${PUBLIC_BASE_URL}

# --- Caddy (public edge) ---
CADDY_DOMAIN=${CADDY_DOMAIN}
CADDY_HTTP_PORT=${CADDY_HTTP_PORT}
CADDY_HTTPS_PORT=${CADDY_HTTPS_PORT}
SHORT_PLAYLIST_PATH=${SHORT_PLAYLIST_PATH}

# --- Logging ---
LOG_LEVEL=${LOG_LEVEL}
LOG_FORMAT=${LOG_FORMAT}
TELEGRAM_LOG_LEVEL=${TELEGRAM_LOG_LEVEL}

# --- Discovery ---
SCAN_DIALOG_DELAY_SECONDS=0.35
FULL_SCAN_INTERVAL_SECONDS=3600

# --- Ingest ---
MAX_CONCURRENT_INGESTS=${MAX_CONCURRENT_INGESTS}
IDLE_GRACE_SECONDS=${IDLE_GRACE_SECONDS}
INGEST_START_STAGGER_SECONDS=3
INGEST_INPUT_REJOIN_SECONDS=30
INGEST_REBUFFER_SECONDS=3
INGEST_STARTUP_GRACE_SECONDS=15
INGEST_OUTPUT_RECOVER_COOLDOWN_SECONDS=1
INGEST_RECOVERY_HOLD_SECONDS=90

# --- MediaMTX / RTMP (Docker internal hostnames) ---
RTMP_BASE_URL=rtmp://mediamtx:1935/live
MEDIAMTX_HLS_URL=http://mediamtx:8888
MEDIAMTX_HLS_CDN_SECRET=${MEDIAMTX_HLS_CDN_SECRET}
MEDIAMTX_RTMP_PORT=1935
MEDIAMTX_HLS_PORT=8888
MEDIAMTX_LOG_LEVEL=info
EOF

	chmod 600 "$ENV_FILE"
	print_summary

	if ! have_docker; then
		echo
		echo "Docker not found — when ready:"
		echo "  cd ${INSTALL_DIR} && docker compose up -d"
		echo "  cd ${INSTALL_DIR} && docker compose run --rm -it tgtv tgtv login"
		return 0
	fi

	echo
	run_login_if_needed
}

run_telegram_menu() {
	if ! have_docker; then
		echo "Docker not found." >&2
		return 1
	fi

	check_telegram_session
	if telegram_logged_in; then
		run_logout
		echo "Telegram: logged out."
	else
		run_login
		finish_telegram_login
	fi
}

run_menu() {
	while true; do
		load_existing
		echo
		echo "=== TGTV ==="
		echo "Install: ${INSTALL_DIR}"
		if [[ -n "${PUBLIC_BASE_URL:-}" && -n "${SHORT_PLAYLIST_PATH:-}" ]]; then
			echo "Playlist: ${PUBLIC_BASE_URL}${SHORT_PLAYLIST_PATH}"
		fi
		if have_docker; then
			echo
			echo "Containers:"
			dc ps --format '  {{.Name}}: {{.Status}}' 2>/dev/null || echo "  (none)"
			check_telegram_session
			echo "Telegram: ${TELEGRAM_SESSION_DETAIL:-unknown}"
		fi

		local n=0
		local menu_start=0 menu_stop=0 menu_restart=0 menu_reconfig=0 menu_uninstall=0 menu_rotate=0 menu_telegram=0
		local show_stack=false
		local stack_up=false
		if ! have_docker || telegram_logged_in; then
			show_stack=true
		fi
		if [[ "$show_stack" == true ]] && stack_running; then
			stack_up=true
		fi

		echo
		if [[ "$show_stack" == true ]]; then
			if [[ "$stack_up" != true ]]; then
				n=$((n + 1)); menu_start=$n; echo "  ${n}) Start"
			fi
			if [[ "$stack_up" == true ]]; then
				n=$((n + 1)); menu_stop=$n; echo "  ${n}) Stop"
				n=$((n + 1)); menu_restart=$n; echo "  ${n}) Restart"
			fi
		fi
		n=$((n + 1)); menu_reconfig=$n; echo "  ${n}) Reconfigure"
		n=$((n + 1)); menu_rotate=$n; echo "  ${n}) Rotate secrets"
		if have_docker; then
			n=$((n + 1)); menu_telegram=$n
			if telegram_logged_in; then
				echo "  ${n}) Logout Telegram"
			else
				echo "  ${n}) Login Telegram"
			fi
		fi
		n=$((n + 1)); menu_uninstall=$n; echo "  ${n}) Uninstall"
		echo "  q) Quit"
		echo
		local choice=""
		read -r -p "Choice: " choice

		case "$choice" in
			q|Q|quit|exit)
				break
				;;
		esac

		if [[ "$choice" == "$menu_start" || "$choice" == "start" || "$choice" == "Start" ]]; then
			if [[ "$menu_start" -eq 0 ]]; then
				echo "Invalid choice." >&2
				continue
			fi
			if ! have_docker; then
				echo "Docker not found." >&2
				continue
			fi
			check_telegram_session
			if ! telegram_logged_in; then
				echo "Telegram login required." >&2
				continue
			fi
			start_stack
		elif [[ "$choice" == "$menu_stop" || "$choice" == "stop" || "$choice" == "Stop" ]]; then
			if [[ "$menu_stop" -eq 0 ]]; then
				echo "Invalid choice." >&2
				continue
			fi
			if ! have_docker; then
				echo "Docker not found." >&2
				continue
			fi
			check_telegram_session
			if ! telegram_logged_in; then
				echo "Telegram login required." >&2
				continue
			fi
			stop_stack
		elif [[ "$choice" == "$menu_restart" || "$choice" == "restart" || "$choice" == "Restart" ]]; then
			if [[ "$menu_restart" -eq 0 ]]; then
				echo "Invalid choice." >&2
				continue
			fi
			if ! have_docker; then
				echo "Docker not found." >&2
				continue
			fi
			check_telegram_session
			if ! telegram_logged_in; then
				echo "Telegram login required." >&2
				continue
			fi
			restart_stack
		elif [[ "$choice" == "$menu_reconfig" || "$choice" == "reconfig" || "$choice" == "Reconfigure" ]]; then
			run_configure
		elif [[ "$choice" == "$menu_uninstall" || "$choice" == "uninstall" || "$choice" == "Uninstall" ]]; then
			run_uninstall && break
		elif [[ "$choice" == "$menu_rotate" || "$choice" == "rotate" || "$choice" == "Rotate" ]]; then
			run_rotate_secrets
		elif [[ "$choice" == "$menu_telegram" || "$choice" == "telegram" || "$choice" == "login" || "$choice" == "logout" || "$choice" == "Login" || "$choice" == "Logout" ]]; then
			if [[ "$menu_telegram" -eq 0 ]]; then
				echo "Invalid choice." >&2
				continue
			fi
			run_telegram_menu
		else
			echo "Invalid choice." >&2
		fi
	done
}

main() {
	echo "=== TGTV ==="
	echo "Install directory: ${INSTALL_DIR}"
	echo

	ensure_install_dir
	load_existing

	if [[ ! -f "$ENV_FILE" ]]; then
		run_configure
	else
		run_menu
	fi
}

main "$@"
