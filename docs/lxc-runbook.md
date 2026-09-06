# LXC deployment runbook

Target: Debian LXC with no application state preinstalled.

## 1. Prepare host secrets

```bash
sudo mkdir -p /etc/parquet
sudo cp private_key.pem /etc/parquet/private_key.pem
sudo cp public_key.pem /etc/parquet/public_key.pem
```

Do not place a GitHub token yet unless a **private** runtime channel has been selected.

## 2. Clone and install

```bash
git clone https://github.com/Moncloa/Parquet.git
cd Parquet
sudo ./install.sh
```

The installer is idempotent and preserves `/etc/parquet/parquet.yaml` on upgrades.

## 3. Validate

```bash
systemctl status parquet --no-pager
curl -fsS http://127.0.0.1:8787/health
curl -fsS http://127.0.0.1:8787/status
journalctl -u parquet -n 100 --no-pager
```

Expected mode is `shadow`.

## 4. Upgrade

```bash
cd Parquet
./scripts/update.sh
```

## Monday acceptance criteria

The initial deployment is considered online when:

- service starts automatically after LXC restart;
- `/health` reports `status=ok`;
- `/status` shows the structural market-opening reviews;
- SQLite state survives a service restart;
- mode is `shadow`;
- GitHub runtime bridge remains disabled until a private channel is validated;
- no code path can send a real eToro order.
