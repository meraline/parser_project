# Auto Reviews Parser

A parser for collecting car reviews and logs from **Drom.ru** and **Drive2.ru**. The project now ships with Docker setup, Prometheus metrics and optional Redis caching.

## Quick start

1. Create `.env` file (see `.env.example`).
2. Install dependencies and run parser:

```bash
pip install -r requirements.txt
python auto_reviews_parser.py init
python auto_reviews_parser.py parse --sources 3
```


## Health check

To verify that the parser and database are reachable run the health check
command. It returns a small JSON payload with overall status and a link to
the Prometheus metrics endpoint.

```bash
python cli/main.py health
```

## Environment variables

The application reads configuration from environment variables (or a `.env` file). Key settings:

| Variable | Default | Description |
| --- | --- | --- |
| `DB_PATH` | `auto_reviews.db` | Path to the SQLite database |
| `PROMETHEUS_PORT` | `8000` | Port where Prometheus metrics are exposed |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `MAX_WORKERS` | `4` | Number of worker threads for parallel parsing |


## Prometheus metrics

The parser exposes metrics using [`prometheus-client`](https://github.com/prometheus/client_python). By default metrics are available on `http://localhost:${PROMETHEUS_PORT}`.

## Docker

Inside the `docker/` folder there is a ready to use `docker-compose.yml`:

```bash
cd docker
docker-compose up --build
```

This starts the parser, Prometheus and Redis (for caching). Metrics are scraped automatically by Prometheus.

## Packaging

The project includes a minimal `setup.py` for packaging and distribution. Install in editable mode with:

```bash
pip install -e .
```

## Running tests

```bash
pytest
```

## License

MIT

