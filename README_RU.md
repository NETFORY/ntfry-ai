# ntfry-ai - OpenAI-совместимый API-прокси в AI-рой NETFORY

Лёгкая консольная утилита: поднимает HTTP-сервер (по умолчанию `0.0.0.0:9871`)
и проксирует стандартные OpenAI/LM Studio-запросы в децентрализованную сеть
NETFORY (iroh QUIC, ALPN `netfory/ai/1`).

## Быстрый старт

```bash
cargo build --release
./target/release/ntfry-ai keygen --name dev     # sk-ntfry-...
./target/release/ntfry-ai serve                 # или просто ./ntfry-ai
```

```bash
curl http://127.0.0.1:9871/v1/models -H "Authorization: Bearer sk-ntfry-..."

curl http://127.0.0.1:9871/v1/chat/completions \
  -H "Authorization: Bearer sk-ntfry-..." -H "Content-Type: application/json" \
  -d '{"model":"auto","messages":[{"role":"user","content":"Привет, что ты умеешь?"}],"stream":true}'
```

Работает с любым OpenAI SDK: `base_url = "http://host:9871/v1"`, `api_key = "sk-ntfry-..."`.
