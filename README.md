# EduStream — Real-Time Learning Data Pipeline

> A hands-on big-data project: simulates real-time student learning events,
> streams them through **Apache Kafka**, and stores them in **HDFS**.

---

## What this project does

```
[Python Producer]  →  [Kafka]  →  [Python Consumer]  →  [HDFS / Local files]
  (generates             (message      (cleans &             (stored as
   fake events)           broker)       transforms)           JSON files)
```

1. **Producer** — generates fake learning events (quiz attempts, logins, video views…)
2. **Kafka** — acts like a post office: receives messages and holds them until consumed
3. **Consumer** — reads each event, anonymizes student IDs, and writes to storage
4. **HDFS** — distributed file system; falls back to local folder if not running

---

## Project structure

```
edustream/
├── docker-compose.yml          ← starts Kafka + Hadoop with one command
├── requirements.txt            ← Python dependencies
├── analyze.py                  ← prints a summary report from stored data
│
├── edustream/                  ← main Python package
│   ├── config.py               ← ALL settings in one place
│   ├── models.py               ← LearningEvent data shape + validation
│   ├── producer.py             ← generates & sends events to Kafka
│   ├── consumer.py             ← reads from Kafka, transforms, writes to HDFS
│   ├── hdfs_writer.py          ← batches and writes events to HDFS (or local)
│   └── utils/
│       ├── logging_cfg.py      ← colored terminal logs
│       └── retry.py            ← automatic retry with exponential backoff
│
├── scripts/
│   ├── demo_no_docker.py       ← run the full pipeline WITHOUT Docker
│   └── setup_kafka_topic.py    ← creates the Kafka topic (run once)
│
└── tests/
    ├── test_models.py          ← tests for LearningEvent
    └── test_consumer.py        ← tests for processing & anonymization
```

---

## Day-by-day plan

| Day | What you do | Command |
|-----|-------------|---------|
| 1   | Install Python dependencies, run demo without Docker | `pip install -r requirements.txt` then `python scripts/demo_no_docker.py` |
| 2   | Run all tests, read the code | `python -m pytest tests/ -v` |
| 3   | Start Docker, spin up Kafka + HDFS | `docker-compose up -d` |
| 4   | Run producer + consumer with real Kafka | See "With Docker" section below |
| 5   | Generate report, polish README | `python analyze.py` |

---

## Option A — Quick demo (no Docker needed)

This is the easiest way to see the pipeline work immediately.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the full pipeline demo (generates 200 events, saves locally)
python scripts/demo_no_docker.py

# 3. See the analysis report
python analyze.py
```

Data is saved to `./data/hdfs_fallback/` as JSON files.

---

## Option B — Full setup with Docker (real Kafka + HDFS)

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### Step 1 — Start all services
```bash
docker-compose up -d
```

Wait about 30 seconds, then check everything is running:
```bash
docker-compose ps
```

You should see all services with status `Up`.

### Step 2 — Open the dashboards (optional but useful)
| Dashboard | URL | What you see |
|-----------|-----|--------------|
| Kafka UI | http://localhost:8080 | Topics, messages, consumers |
| HDFS UI  | http://localhost:9870 | Files stored in HDFS |

### Step 3 — Create the Kafka topic (run once)
```bash
python scripts/setup_kafka_topic.py
```

### Step 4 — Install Python dependencies
```bash
pip install -r requirements.txt
```

### Step 5 — Open TWO terminal windows

**Terminal 1 — Start the consumer** (reads from Kafka, writes to HDFS):
```bash
python -m edustream.consumer
```

**Terminal 2 — Start the producer** (generates events):
```bash
python -m edustream.producer
```

You will see events flowing in real time in both terminals.

### Step 6 — Analyze stored data
```bash
python analyze.py
```

### Step 7 — Stop everything
```bash
docker-compose down
```

---

## Running the tests

```bash
python -m pytest tests/ -v
```

Expected output: **17 passed**.

---

## Configuration

All settings are in `edustream/config.py`. You can change:

| Setting | Default | What it controls |
|---------|---------|-----------------|
| `EVENTS_PER_SECOND` | `2` | How fast events are generated |
| `BATCH_SIZE` | `50` | Events per HDFS write |
| `STUDENT_POOL_SIZE` | `50` | Number of simulated students |
| `ANONYMIZATION_SALT` | `"edustream_secret_2024"` | Salt for student ID hashing |

---

## Sample event (raw, before processing)

```json
{
  "event_id": "a1b2c3d4-...",
  "student_id": "STU-00042",
  "event_type": "quiz_attempt",
  "timestamp": "2024-06-01T12:00:00+00:00",
  "metadata": {
    "quiz_id": "QZ-201",
    "score": 85,
    "max_score": 100,
    "duration_s": 420
  }
}
```

## Sample event (after processing — as stored in HDFS)

```json
{
  "event_id": "a1b2c3d4-...",
  "student_id": "ANON-F93FFFCFF66D5CCE",
  "event_type": "quiz_attempt",
  "timestamp": "2024-06-01T12:00:00+00:00",
  "processed_at": "2024-06-01T12:00:00.123456+00:00",
  "metadata": {
    "quiz_id": "QZ-201",
    "score": 85,
    "max_score": 100,
    "duration_s": 420
  },
  "pipeline_version": "1.0"
}
```

Note: `student_id` is now an anonymized SHA-256 hash — the original cannot be recovered.

---

## Error handling

| Error | What happens |
|-------|-------------|
| Kafka not reachable | Retries up to 5 times with exponential backoff (1s → 2s → 4s …) |
| Bad/malformed event | Logged as a warning and skipped — pipeline never crashes |
| HDFS not reachable | Automatically falls back to local folder |
| HDFS write fails | Retried up to 5 times before alerting |

---

## Technologies used

| Tool | Purpose |
|------|---------|
| Python 3.12 | Producer, consumer, processing logic |
| Apache Kafka | Real-time message streaming |
| HDFS (Hadoop) | Distributed file storage |
| Docker | Runs Kafka + HDFS without manual installation |
| kafka-python | Python library for Kafka |
| hdfs | Python library for HDFS |
| Faker | Generates realistic fake student data |
| pytest | Automated testing |
