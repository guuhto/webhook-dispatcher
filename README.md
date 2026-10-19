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

## Testes

```sh
go test -race ./...
```

> Observação: o worker de entrega (`internal/dispatcher`) será ligado no `main` após o merge da t2.
