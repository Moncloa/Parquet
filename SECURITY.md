# Security policy

## Secrets

No live secret belongs in the Git repository.

Host contract:

```text
/etc/parquet/private_key.pem
/etc/parquet/public_key.pem
/etc/parquet/github_token
/etc/parquet/parquet.yaml
```

`install.sh` must not generate or overwrite the private/public key pair.

## Runtime communication

The source repository can be public, but **runtime trading communication must not use a public issue or pull request**. Market snapshots, portfolio state, trade proposals and execution results may be sensitive.

The GitHub bridge therefore ships disabled by default. Enable it only when the selected runtime PR is private to the intended participants. If the source repository remains public, use a separate private repository or another private message bus for runtime communication.

## Trading safety

Real-money execution is not implemented in V0.2. Future broker adapters must preserve these invariants:

- stop-loss required for every new position;
- deterministic risk checks cannot be bypassed by ChatGPT;
- stale/expired proposals are rejected;
- ambiguous broker outcomes are reconciled before retrying;
- loss limits can disable new entries without LLM involvement;
- existing broker-side protection remains effective if ChatGPT/GitHub/Parquet is unavailable;
- credentials are scoped to the smallest possible portfolio/account surface.
