# webhook-dispatcher

A minimal, Svix-style webhook dispatcher in Go. It accepts webhooks over HTTP, queues them
in memory and delivers them (POST) to their destination, retrying with exponential backoff
and parking events that keep failing in a dead letter list.

## How this was built

This project is an experiment with [AgentCraft](https://github.com/blendi-remade/agentcraft),
a tool that runs a team of Claude agents on a git repository and shows them working inside a
Minecraft studio.

I set the goals, answered the agents' questions, approved or denied their commands, reviewed
every diff and merged (or sent back) each change. The code itself was written by the agents:
a lead that planned the work and two workers that implemented each task in its own git
worktree.

Some of the calls I made along the way:

- asking for `Dequeue` to take a `context.Context`, so the worker can shut down cleanly;
- separating the API request type from the internal `Event`, so clients can't set `attempts`;
- moving all comments, logs and identifiers to English.

The goal was to see how far a multi-agent setup can take a small backend service, and what a
human reviewer still needs to catch along the way. Turns out: context cancellation, input
validation, and a test named `TestDequeueRetornaCtxErrNoPrazoExpirado`.

## Running

```sh
go run ./cmd/dispatcher                 # listens on :8080
go run ./cmd/dispatcher -addr :9000     # or PORT_ADDR=:9000
```

The server shuts down gracefully on `Ctrl+C` / `SIGTERM`.

## Sending a webhook

```sh
curl -i -X POST http://localhost:8080/webhooks \
  -H 'Content-Type: application/json' \
  -d '{"url": "https://example.com/hook", "payload": {"order": 42}}'
```

Response `202 Accepted`:

```json
{"id": "9f2c0e5b7a1d4c3e8b6a5d4c3b2a1f00"}
```

| Status | When |
|--------|------|
| 202 | webhook queued |
| 400 | invalid JSON, `url` not http/https, or missing `payload` |
| 405 | method other than POST |

## Retries and configuration

Transient failures (network errors, 5xx, 429) are retried with exponential backoff:
`base * multiplier^n`, capped at the maximum. Other 4xx responses are not retried.
Configure with environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKOFF_BASE` | `30s` | wait after the first failure (`time.ParseDuration` format) |
| `BACKOFF_MULTIPLIER` | `2` | growth factor (>= 1) |
| `BACKOFF_MAX` | `1h` | maximum wait (>= `BACKOFF_BASE`) |
| `MAX_ATTEMPTS` | `8` | total attempts per event (> 0) |

Invalid values stop the server from starting; the effective configuration is logged at startup.

## Dead letters

Events that run out of attempts go to an in-memory list (lost on restart), available at
`GET /webhooks/dead`:

```json
[
  {
    "event": {"id": "9f2c0e5b7a1d4c3e8b6a5d4c3b2a1f00", "url": "https://example.com/hook", "payload": {"order": 42}, "attempts": 8},
    "error": "dispatcher: attempts exhausted (8): status 500",
    "failed_at": "2026-10-19T12:00:00Z"
  }
]
```

With no dead events the response is `[]`.

## Tests

```sh
go test -race ./...
```

## Roadmap

- Redrive: `POST /webhooks/dead/{id}/retry`
- HMAC-signed payloads (`X-Webhook-Signature`)
- Persistent queue (SQLite)

No villagers were harmed in the making of this dispatcher.
