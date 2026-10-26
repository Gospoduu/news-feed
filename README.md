# news-feed

FastAPI service that stores news in PostgreSQL, serves the feed, and requests company insights over Kafka.

## Run locally with Docker Compose

Start the full local stack from the Alfa-Hack-2026 project root:

```sh
bash scripts/dev-up.sh
```

The API is available at `http://localhost:8002`; interactive API docs are at `/docs`. Kafka UI is available at `http://localhost:8080`. The stack uses one Kafka broker from news-parser on the shared `alfa-services` Docker network, avoiding duplicate brokers and host-port collisions. News-parser provisions all three topics. On first startup, news-feed creates its database schema and imports the bundled CSV datasets.

To stop the services while keeping database, Redis, and Kafka data:

```sh
bash scripts/dev-down.sh
```

To run news-feed by itself, first start news-parser's Compose stack so it creates the shared network and Kafka broker, then run `docker compose up --build -d --remove-orphans` from this directory.

The Kafka topics are shared with the adjacent services:

| Purpose | Topic |
| --- | --- |
| Parsed news from news-parser | `news.created` |
| Insight requests to the analysis agent | `insight.requested` |
| Completed insight responses | `insight.completed` |

The news-parser publisher sends `news.parsed.v1` events with the RSS source metadata (`name`, `rss_url`, `site_url`) and article batch. The same event is consumed by the insight agent from `news.created`.

Topic names and exposed ports can be overridden with environment variables (`NEWS_TOPIC`, `ANALYSIS_REQUEST_TOPIC`, `ANALYSIS_RESPONSE_TOPIC`, `NEWS_FEED_APP_PORT`). PostgreSQL credentials and database name can be set with `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`.

## API

- `GET /news/feed?limit=50&offset=0` — news from the last three days.
- `GET /persons/{person_id}/companies?limit=50&offset=0` — companies linked to a person.
- `GET /companies/{company_id}/insight` — cached insight or `202 processing` while analysis runs.
