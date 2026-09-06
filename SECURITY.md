# Security policy

## Secrets

No live secret belongs in the Git repository.

Host contract:

```text
/etc/parquet/github_deploy_key
/etc/parquet/github_deploy_key.pub
/etc/parquet/private_key.pem
/etc/parquet/public_key.pem
/etc/parquet/github_token
/etc/parquet/parquet.yaml
```

Credential roles are intentionally separated:

- `github_deploy_key`: SSH deploy key for read-only clone/pull of `Moncloa/Parquet`.
- `github_token`: future runtime ChatGPT/GitHub bridge only; it must be fine-grained and limited to the minimum repository permissions required.
- `private_key.pem` / `public_key.pem`: broker/auth material. Their exact eToro mapping must be validated before broker integration is enabled.

`install.sh` must not generate or overwrite broker credentials or the GitHub deploy private key.

The deploy key must be registered in GitHub without **Allow write access**. The LXC does not need permission to modify Parquet's source code.

## SSH host verification

Parquet pins GitHub's published ED25519 host key in `deploy/github_known_hosts` and installs it as `/etc/parquet/github_known_hosts`.

Expected GitHub ED25519 fingerprint:

```text
SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU
```

The first clone must verify this fingerprint before trusting a key obtained from the network.

## Runtime communication

The source repository is intended to remain private because runtime market snapshots, portfolio state, trade proposals and execution results may be sensitive.

The GitHub runtime bridge still ships disabled by default. Making the repository private is necessary but not sufficient: enable the bridge only after its event flow and token permissions have been validated in SHADOW mode.

Do not reuse the read-only SSH deploy key as a runtime-write credential.

## Trading safety

Real-money execution is not implemented in V0.3. Future broker adapters must preserve these invariants:

- stop-loss required for every new position;
- deterministic risk checks cannot be bypassed by ChatGPT;
- stale/expired proposals are rejected;
- ambiguous broker outcomes are reconciled before retrying;
- loss limits can disable new entries without LLM involvement;
- existing broker-side protection remains effective if ChatGPT/GitHub/Parquet is unavailable;
- credentials are scoped to the smallest possible portfolio/account surface.
