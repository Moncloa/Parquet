# LXC deployment runbook

Target: fresh **Debian 13** unprivileged LXC. Initial mode is **SHADOW**; no real broker order is sent.

## Recommended Proxmox resources

- 2 vCPU
- 2 GiB RAM
- 512 MiB swap
- 16 GiB disk
- `vmbr0`
- start at boot enabled
- nesting/FUSE/keyctl disabled
- no inbound Internet port required

## 1. Bootstrap Git access for the private repository

Install only what is required to perform the first clone:

```bash
apt-get update
apt-get install -y git openssh-client ca-certificates
mkdir -p /etc/parquet
chmod 0750 /etc/parquet
```

### Create or restore the read-only deploy key

Recommended for a new LXC:

```bash
ssh-keygen -t ed25519 -N '' \
  -f /etc/parquet/github_deploy_key \
  -C 'parquet-lxc-deploy'
chmod 0600 /etc/parquet/github_deploy_key
chmod 0644 /etc/parquet/github_deploy_key.pub
cat /etc/parquet/github_deploy_key.pub
```

Add that **public** key in GitHub:

`Moncloa/Parquet -> Settings -> Deploy keys -> Add deploy key`

Do **not** enable write access.

If rebuilding an LXC from backup, restoring the existing `/etc/parquet/github_deploy_key` is also supported.

### Verify GitHub's SSH host key

Create a temporary known-hosts file:

```bash
ssh-keyscan -t ed25519 github.com > /etc/parquet/github_known_hosts
ssh-keygen -lf /etc/parquet/github_known_hosts
```

The ED25519 fingerprint must be:

```text
SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU
```

If it differs, stop. Do not clone.

### Clone

```bash
GIT_SSH_COMMAND='ssh -i /etc/parquet/github_deploy_key -o IdentitiesOnly=yes -o UserKnownHostsFile=/etc/parquet/github_known_hosts -o StrictHostKeyChecking=yes' \
  git clone git@github.com:Moncloa/Parquet.git
cd Parquet
```

After the first clone, `install.sh` replaces `github_known_hosts` with the pinned GitHub ED25519 key shipped by Parquet and creates `/etc/parquet/ssh_config` for unattended read-only updates.

## 2. Place broker/runtime secrets

The host contract is:

```text
/etc/parquet/github_deploy_key      # Git clone/pull only; root:root 0600
/etc/parquet/github_deploy_key.pub  # optional; root:root 0644
/etc/parquet/private_key.pem        # broker/auth material; meaning finalized with eToro adapter
/etc/parquet/public_key.pem         # broker/auth material; meaning finalized with eToro adapter
/etc/parquet/github_token           # runtime bridge only; NOT needed for initial SHADOW deploy
```

The deploy key and the runtime GitHub token are deliberately separate credentials.

Broker key files may be omitted for the first SHADOW deployment; the installer will warn but still start. Never commit any of these files.

## 3. Install

The installer must run as root. Minimal Debian LXCs commonly do not install `sudo`, so when the shell prompt is already `root@parquet`, run:

```bash
./install.sh
```

If working as a regular administrative user on a system that has `sudo`, the equivalent is:

```bash
sudo ./install.sh
```

The installer is idempotent. It creates the `parquet` service user, Python environment, configuration, SQLite state directory, pinned GitHub SSH configuration and `parquet.service`. Existing `/etc/parquet/parquet.yaml` and secret files are preserved.

## 4. Validate

```bash
systemctl status parquet --no-pager
curl -fsS http://127.0.0.1:8787/health
curl -fsS http://127.0.0.1:8787/status
journalctl -u parquet -n 100 --no-pager
```

Expected mode is `shadow`.

Validate private-repository access too:

```bash
GIT_SSH_COMMAND='ssh -F /etc/parquet/ssh_config' \
  git -C "$PWD" ls-remote origin HEAD
```

## 5. Upgrade

The update script also requires root. From a root shell in the checkout:

```bash
./scripts/update.sh
```

Or, from a regular administrative account where `sudo` is installed:

```bash
sudo ./scripts/update.sh
```

The update script uses `/etc/parquet/github_deploy_key`, performs a fast-forward-only pull, then reruns the idempotent installer.

## Monday acceptance criteria

The initial deployment is considered online when:

- service starts automatically after LXC restart;
- `/health` reports `status=ok`;
- `/status` shows structural Asia/Europe/Wall Street reviews;
- SQLite state survives a service restart and is safe across API/worker threads;
- mode is `shadow`;
- private-repository `git pull` works through the read-only deploy key;
- GitHub runtime bridge remains disabled until its permissions/event flow are validated;
- no code path can send a real eToro order.
