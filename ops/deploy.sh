#!/usr/bin/env bash
# Deploy spain-map to VPS. Run on the VPS as root.
# First-time setup installs the systemd unit + Caddyfile; subsequent runs just pull + restart.
set -euo pipefail

REPO_URL="https://github.com/wbtechpro/spain.git"
APP_DIR="/opt/spain-map"
FRONT_DIR="/var/www/spain-map"
UV="/root/.local/bin/uv"

# 1. pull repo (clone on first run)
if [[ ! -d "$APP_DIR/.git" ]]; then
    git clone "$REPO_URL" "$APP_DIR"
else
    git -C "$APP_DIR" fetch --quiet origin main
    git -C "$APP_DIR" reset --hard origin/main
fi

# 2. regenerate parquet from whatever JSON is in data/
"$UV" run --project "$APP_DIR/server" "$APP_DIR/etl/to_parquet.py"

# 3. install/update systemd unit + Caddyfile if they changed
install -m 0644 "$APP_DIR/ops/spain-map.service" /etc/systemd/system/spain-map.service
install -m 0644 "$APP_DIR/ops/Caddyfile" /etc/caddy/Caddyfile

# 4. install server deps (creates .venv on first run, no-op otherwise)
"$UV" sync --project "$APP_DIR/server" --quiet

# 5. frontend: mirror index.html + data/ into $FRONT_DIR
mkdir -p "$FRONT_DIR"
rsync -a --delete \
    --include='index.html' \
    --include='data/' --include='data/**' \
    --exclude='*' \
    "$APP_DIR/" "$FRONT_DIR/"
chown -R caddy:caddy "$FRONT_DIR" "$APP_DIR"

# 6. reload systemd + bounce services
systemctl daemon-reload
systemctl enable --now spain-map
systemctl restart spain-map
caddy validate --config /etc/caddy/Caddyfile
systemctl reload caddy

echo "--- status ---"
systemctl is-active spain-map caddy
curl -sf http://127.0.0.1:8000/api/health && echo
