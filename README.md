# Claude Code — Token & Cost Monitoring

Stack autocontingut per analitzar on es gasten els tokens de Claude Code:
parseja els JSONL de `~/.claude/projects/`, els fica en SQLite i ho serveix
amb Grafana.

## Aixecar

```bash
docker compose up -d --build
```

Obre <http://localhost:3000>. Login deshabilitat (anonymous Admin), va directe.

El dashboard "Claude Code — Token & Cost" està provisionat automàticament.

## Què mesura

Per cada missatge de l'assistant, captura:

| Camp           | Significat                                                |
|----------------|-----------------------------------------------------------|
| `input_tokens` | Tokens d'input fora de cache                              |
| `output_tokens`| Tokens generats per Claude                                |
| `cache_5m`     | Cache writes amb TTL 5 min (1.25× preu input)             |
| `cache_1h`     | Cache writes amb TTL 1 h (2× preu input)                  |
| `cache_read`   | Cache reads (0.1× preu input — el barat)                  |
| `cost_usd`     | Cost calculat amb preus de LiteLLM                        |
| `project`      | Carpeta del projecte (extret del path)                    |
| `model`        | Ex: `claude-opus-4-7`, `claude-sonnet-4-6`                |
| `session_id`   | Una sessió de Claude Code                                 |

## Panells

- **KPIs**: cost total, tokens, sessions, cache hit ratio del període seleccionat.
- **Cost over time** apilat per projecte (1h buckets).
- **Tokens over time** desglossats per tipus (input/output/cache_create/cache_read).
- **Cost per hour-of-day** per veure quan crames més.
- **Cost per project / per model** (donut).
- **Cost per dia de la setmana**.
- **Top sessions by cost**.

Dos selectors al capçal: `project` i `model`.

## Optimització: on mirar primer

1. **Cache hit ratio < 70%**: estàs perdent diners en cache writes en sessions
   curtes. Considera rondes més llargues per amortitzar el cache_5m.
2. **Cost per project**: el projecte amb més cost relatiu té sessions més
   llargues o més tools heavy → revisa contexts grans (CLAUDE.md gegants,
   directoris on `Read` retorna molt).
3. **Cache_1h elevat**: els writes 1h costen 2× input. Si no estàs reaprofitant
   en >5 min, podries baixar a 5m.

## Arquitectura

```
~/.claude/projects/   →  ingester (Python, cada 5 min)  →  /data/tokens.db  →  Grafana (SQLite plugin)
```

- **ingester** (Python 3.12): re-parseja tots els JSONL nous (dedup per
  `message_id`). Idempotent. Escolta variables d'entorn `INTERVAL_SECONDS`,
  `CLAUDE_PROJECTS_DIR`, `DB_PATH`.
- **Preus**: `pricing.py` baixa el JSON de LiteLLM al primer ús; fallback
  hardcoded.

## Tasques d'operació

```bash
# Logs de l'ingester
docker logs -f token-ingester

# Re-ingest manual (one-shot)
docker compose run --rm ingester python /app/ingest.py --once

# Reset complet
docker compose down -v
```

## Fitxers

```
docker-compose.yml
ingester/
  Dockerfile
  ingest.py        # parser + writer
  pricing.py       # LiteLLM prices + fallback
grafana/provisioning/
  datasources/sqlite.yml
  dashboards/dashboards.yml
  dashboards/tokens.json
```
