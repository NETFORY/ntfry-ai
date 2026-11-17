# ntfry-ai - OpenAI-compatible API proxy in the NETFORY AI swarm

A lightweight console utility: starts an HTTP server (by default `0.0.0.0:9871`)
and proxies standard OpenAI/LM Studio requests to the decentralized
NETFORY network (iroh QUIC, ALPN `netfory/ai/1`).

## Quick start

```bash
cargo build --release
./target/release/ntfry-ai keygen --name dev     # sk-ntfry-...
./target/release/ntfry-ai serve                 # or simply ./ntfry-ai
```

```bash
curl http://127.0.0.1:9871/v1/models -H "Authorization: ******"

curl http://127.0.0.1:9871/v1/chat/completions \
  -H "Authorization: ******" -H "Content-Type: application/json" \
  -d '{"model":"auto","messages":[{"role":"user","content":"Hi, what can you do?"}],"stream":true}'
```

Works with any OpenAI SDK: `base_url = "http://host:9871/v1"`, `api_key = "sk-ntfry-..."`.
