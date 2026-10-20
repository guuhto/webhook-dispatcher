# webhook-dispatcher

Recebe webhooks via HTTP, guarda numa fila em memória e os entrega (POST) no destino.

## Executando

```sh
go run ./cmd/dispatcher                 # escuta em :8080
go run ./cmd/dispatcher -addr :9000     # ou PORT_ADDR=:9000
```

O servidor encerra de forma graciosa com `Ctrl+C` / `SIGTERM`.

## Enviando um webhook

```sh
curl -i -X POST http://localhost:8080/webhooks \
  -H 'Content-Type: application/json' \
  -d '{"url": "https://exemplo.com/hook", "payload": {"pedido": 42}}'
```

Resposta `202 Accepted`:

```json
{"id": "9f2c0e5b7a1d4c3e8b6a5d4c3b2a1f00"}
```

| Status | Quando |
|--------|--------|
| 202 | webhook enfileirado |
| 400 | JSON inválido, `url` que não seja http/https ou `payload` ausente |
| 405 | método diferente de POST |

## Retentativas e configuração

Entregas que falham de forma transitória (erro de rede, 5xx, 429) são
repetidas com backoff exponencial: `base * multiplicador^n`, limitado ao teto.
Respostas 4xx (exceto 429) não são repetidas. Configure por variáveis de ambiente:

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `BACKOFF_BASE` | `30s` | espera após a primeira falha (formato `time.ParseDuration`) |
| `BACKOFF_MULTIPLIER` | `2` | fator de crescimento (>= 1) |
| `BACKOFF_MAX` | `1h` | teto da espera (>= `BACKOFF_BASE`) |
| `MAX_ATTEMPTS` | `8` | total de tentativas por evento (> 0) |

Valores inválidos impedem o início do servidor; a configuração efetiva é
registrada no log ao iniciar.

## Dead letter

Eventos que esgotam as tentativas vão para uma lista em memória (perdida ao
reiniciar), consultável em `GET /webhooks/dead`:

```json
[
  {
    "event": {"id": "9f2c0e5b7a1d4c3e8b6a5d4c3b2a1f00", "url": "https://exemplo.com/hook", "payload": {"pedido": 42}, "attempts": 8},
    "error": "dispatcher: attempts exhausted (8): status 500",
    "failed_at": "2026-10-19T12:00:00Z"
  }
]
```

Sem eventos mortos a resposta é `[]`.

## Testes

```sh
go test -race ./...
```
