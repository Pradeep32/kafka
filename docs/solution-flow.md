# Kafka Data Replication Challenge — Solution Flow

## Architecture

```
Primary Kafka ──MM2──> Standby Kafka
(9092)                (9094)
    │                     │
commit-log          primary.commit-log
```

## Components

| Component | Purpose |
|-----------|---------|
| `primary-kafka` | Source cluster (KRaft, single node) |
| `standby-kafka` | DR target cluster (KRaft, single node) |
| `mirrormaker2` | Enhanced MM2 with truncation detection |
| `commit-log-producer` | Generates synthetic JSON events |
| `init-topics` | Creates `commit-log` on primary |

## Custom Logic Flow

### 1. Offset Tracking (TruncationDetector)
```
poll() returns records → store lastRecord.offset() + 1 as expectedOffset per partition
```

### 2. Gap Detection (MirrorSourceTask.poll)
```
next poll → first record offset > expectedOffset?
  YES → call beginningOffsets() to check broker state
  NO  → normal replication, update expectedOffset
```

### 3. Truncation vs Reset Decision
```
earliestAvailableOffset > expectedOffset → LOG TRUNCATION → throw LogTruncationException
earliestAvailableOffset == 0 AND expectedOffset > 0 → TOPIC RESET → clear tracking, seekToBeginning
```

### 4. OffsetOutOfRange Recovery
```
OffsetOutOfRangeException caught → seekToBeginning for affected partitions → reset tracking
```

## Bugs Fixed During Implementation

| # | Bug | Root Cause | Fix |
|---|-----|-----------|-----|
| 1 | MM2 crash loop | ENTRYPOINT passed script+config as single arg | Split into ENTRYPOINT + CMD |
| 2 | GetOffsetShell returns 0 | Class removed in Kafka 4.0.0 | Use `kafka-get-offsets.sh` |
| 3 | Docker Hub pull denied | Invalid secrets, images never pushed | Build images locally on runner |
| 4 | `docker compose pause` fails | MM2 restarting from crash loop | Fixed by bug #1 |

## Verification Scenarios

### Scenario 1: Normal Replication
1. Start primary + standby Kafka
2. Create `commit-log` topic (retention=60s)
3. Start MM2
4. Produce 1000 messages
5. Wait 30s → verify 1000 messages on standby

### Scenario 2: Truncation Detection
1. Start Kafka, create topic, produce 500 messages
2. Wait 90s for retention to delete messages
3. Produce 100 more messages
4. Start MM2 → should detect offset gap and throw LogTruncationException

### Scenario 3: Topic Reset Recovery
1. Start Kafka, produce 200 messages, start MM2
2. Pause MM2, delete + recreate `commit-log`
3. Produce 300 new messages
4. Resume MM2 → should detect reset, seek to beginning, replicate new messages

## CI/CD Flow (GitHub Actions)

```
Push to enhanced-mm2-v2
  ↓
Checkout code
  ↓
Pull apache/kafka:4.0.0
  ↓
Setup Java 17
  ↓
Build MM2 jar (./gradlew :connect:mirror:jar)
  ↓
Build producer image (Dockerfile.producer)
  ↓
Build enhanced-mm2 image (Dockerfile.mm2)
  ↓
Run scripts/run_challenge.sh
  ↓
Upload run_challenge_output.log as artifact
```

## Key Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | All services definition |
| `scripts/run_challenge.sh` | Test orchestration (3 scenarios) |
| `.github/Dockerfile.mm2` | Enhanced MM2 image build |
| `.github/Dockerfile.producer` | Producer image build |
| `.github/workflows/run-challenge.yml` | CI verification workflow |
| `docker/mm2.properties` | MM2 replication config |
| `commit-log-producer/src/.../CommitLogProducer.java` | Event producer |
| `commit-log-producer/src/.../EventGenerator.java` | JSON event generator |
