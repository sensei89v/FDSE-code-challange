# FDSE Code Challenge — Implementation

## How to run

```bash
docker compose up --build
```

| Service        | URL                    |
|----------------|------------------------|
| Dashboard      | http://localhost:8888  |
| Transformation | http://localhost:8081  |
| Sink           | http://localhost:8080  |

---

## Architecture

```
Producer  →  MQTT Broker  →  Ingestion  →  Transformation  →  Sink  →  PostgreSQL
                                                                              ↑
                                                                         Notebooks
```

**Producer** — simulates test rigs by publishing individual metric readings to topics `rigs/<rig_id>/measurements/<metric>` at a configurable frequency with optional jitter and dropout.

**MQTT Broker** (eclipse-mosquitto) — queues messages with QoS 1, persistent sessions, and a queue limit of 10,000 messages so readings survive transient downstream slowness.

**Ingestion** — subscribes to `rigs/#` with a persistent session and QoS 1, then forwards each message to the Transformation service via HTTP POST. Retries on 5xx / 429 with exponential backoff.

**Transformation** — groups individual vertical metric messages by `(rig_id, timestamp)` into horizontal rows and flushes to the Sink when any condition is met:
- All 6 metrics have arrived (complete row)
- The metric timestamp is older than `--window` seconds (default 30 s)
- The row has been buffered longer than `--processing-window` seconds in wall-clock time (default 120 s)

Buffer state is persisted to a JSON file on shutdown so in-flight windows survive restarts.

**Sink** — writes horizontal rows to PostgreSQL using an upsert (`INSERT … ON CONFLICT DO UPDATE SET … COALESCE`) so late-arriving partial rows fill in `NULL` columns without overwriting existing values.

**PostgreSQL** — stores the `measures` table with a `UNIQUE (rig_id, timestamp)` constraint. All metric columns are nullable to support partial rows arriving out of order.

**Notebooks** (Marimo) — live dashboard that queries PostgreSQL and plots all 6 metrics per rig over a selectable time window (1 min / 5 min / 1 hour / 1 day), refreshing every 2 seconds.

---

## Key design decisions and trade-offs

**Using HTTP for delivering messages Ingestion → Transformation → Sink**
It is the simplest communication approach, but it requires each service to manage its own state independently. Alternatives: we could use the MQTT broker, Kafka, or RabbitMQ as the transport layer.

**Using as few libraries as possible**
Every service uses the smallest possible set of dependencies. The downside is that HTTP request processing is low-level and verbose, and can lead to complex code as the service grows.

**Transformation flushes only when a new message arrives**
This was done because we expect the frequency of new messages to be much lower than the window size.

**Marimo as the visualisation engine**
I chose Marimo because, from what I had read, it is a more developer-friendly tool.

**Ingestion has no internal queue**
For simplicity, ingestion processes only one message at a time until it is delivered or a 4xx error is received. This architecture can potentially be a bottleneck.

**Windowing by both event time and wall-clock time** — using only event timestamps for flush decisions causes a deadlock when the buffer is full and the same message is retried (time never advances). The `--processing-window` parameter adds a wall-clock deadline so stale windows are always eventually flushed.

**Upsert with COALESCE for late data** — metrics for the same `(rig_id, timestamp)` can arrive after the window has already been flushed (e.g. due to MQTT redelivery). Rather than returning 409, the sink patches `NULL` columns with any newly received values, keeping rows as complete as possible.

**Persistent MQTT session** — with `clean_session=False` and QoS 1, the broker queues messages for the ingestion client while it is offline and delivers them on reconnect, preventing data loss during restarts.

---

## Configuration

### Producer

| Parameter     | Default     | Required | Description                                                      |
|---------------|-------------|----------|------------------------------------------------------------------|
| `--host`      | `localhost` |          | MQTT broker host                                                 |
| `--port`      | `1883`      |          | MQTT broker port                                                 |
| `--loop`      | `false`     |          | Publish continuously until interrupted (default: single tick)    |
| `--frequency` | `100` ms    |          | Interval between publish ticks in milliseconds                   |
| `--id-count`  | `10`        |          | Number of simulated rig IDs (`RIG-01` … `RIG-N`)                 |
| `--accuracy`  | `1.0`       |          | Probability of including a metric per tick (0 = always drop)     |
| `--jitter`    | `0` ms      |          | Max random milliseconds added to each metric's timestamp         |

### Ingestion

| Parameter       | Default       | Required | Description                              |
|-----------------|---------------|----------|------------------------------------------|
| `--host`        | `localhost`   |          | MQTT broker host                         |
| `--port`        | `1883`        |          | MQTT broker port                         |
| `--target-url`  | —             | yes      | Transformation service endpoint URL      |

### Transformation

| Parameter              | Default     | Required | Description                                                        |
|------------------------|-------------|----------|--------------------------------------------------------------------|
| `--host`               | `0.0.0.0`   |          | Address to listen on                                               |
| `--port`               | `8081`      |          | Port to listen on                                                  |
| `--target-url`         | —           | yes      | Sink endpoint URL                                                  |
| `--window`             | `30` s      |          | Max age by metric timestamp before flushing a window               |
| `--processing-window`  | `120` s     |          | Max wall-clock age of a buffered row before flush                  |
| `--max-buffer-size`    | `100`       |          | Max number of open `(rig_id, timestamp)` windows held in memory    |
| `--state-file`         | _(disabled)_|          | Path to JSON file for persisting buffer across restarts            |

### Sink

| Parameter | Default   | Required | Description                   |
|-----------|-----------|----------|-------------------------------|
| `--host`  | `0.0.0.0` |          | Address to listen on          |
| `--port`  | `8080`    |          | Port to listen on             |
| `--db-url`| —         | yes      | PostgreSQL connection URL     |

### Notebooks

| Parameter   | Default | Required | Description               |
|-------------|---------|----------|---------------------------|
| `--db-url`  | —       | yes      | PostgreSQL connection URL |


## Potential issues / known limitations
* All HTTP servers process only one request at a time.
* Ingestion processes only one message at a time.
* The MQTT broker has limited queue capacity — if messages are produced faster than they are processed, messages will eventually be lost.
* The pipeline works with a fixed, known list of metrics.

## Testing strategy

### Happy path

```bash
docker compose up --build
```

Open `localhost:8888` in the browser.

### Network issues between Ingestion → Transformation
Comment out the transformation dependency for ingestion in `docker-compose.yml`.

```bash
docker compose up --build ingestion producer mqtt postgres sink
```

Observe warning logs in the console.

### Network issues between Transformation → Sink
Comment out the sink dependency for transformation in `docker-compose.yml`.

```bash
docker compose up --build ingestion producer mqtt postgres transformation
```

Observe warning logs in the console.

### Simulating a full Transformation buffer
Set `--max-buffer-size` to `"50"` in `docker-compose.yml`.

```bash
docker compose up --build
```

Observe warning logs in the console.

### Transformation state restore
```bash
docker compose up --build
```

Stop docker-compose.

Open `transformation/state.json` and change a metric value to something very large or very small.

```bash
docker compose up --build
```

Open `localhost:8888` in the browser. After a short time, the modified value should appear in the corresponding column in the table.

## Potential scalability
* The producer does not require scaling, but multiple producers can be run in parallel (or the number of simulated rigs can be increased).
* The MQTT broker can be configured to improve throughput.
* Ingestion is one of the potential bottlenecks, but multiple instances can easily be run in parallel.
* For scaling the transformation service there are two options. The general idea behind both is that each transformation instance processes data for only a limited number of rigs:
  * Put nginx or another router in front to route traffic by `rig_id` to the corresponding transformation instance.
  * Implement the same routing logic inside the ingestion service, so it knows exactly which transformation instance to send each message to.
* Ingestion, transformation, and sink can all be improved by using a thread pool.

## What can be improved
* Add unit tests
* Add support for database schema migrations (for when the pipeline schema evolves)
* Make the list of metrics configurable
* Use a message-based transport such as RabbitMQ or Kafka for Ingestion → Transformation → Sink