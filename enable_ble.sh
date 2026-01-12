#!/usr/bin/env bash
set -euo pipefail

echo "[1/5] Install rfkill (if missing)..."
# sudo apt-get update -y
sudo apt-get install -y rfkill bluez

echo "[2/5] rfkill status:"
rfkill list || true

echo "[3/5] Unblock bluetooth..."
sudo rfkill unblock bluetooth || true

echo "[4/5] Enable + start bluetooth service..."
sudo systemctl enable --now bluetooth

echo "[5/5] Verify bluetooth status..."
systemctl status bluetooth --no-pager -l || true

echo
echo "bluetoothctl: power on + show"

bluetoothctl <<'BT'
power on
show
quit
BT

echo
echo "Done. If Controller exists and Powered: yes => BLE is ready."
