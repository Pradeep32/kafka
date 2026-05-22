# Interview Preparation — Kafka Replication Challenge

## Core Concepts to Know

### Q1: What is MirrorMaker 2 and how does it work?

MM2 is Kafka's cross-cluster replication tool built on the Connect framework. It uses Kafka consumers to read from the source cluster and producers to write to the target cluster. It replicates topics, consumer group offsets, and ACLs.

Key components:
- **MirrorSourceConnector**: Reads from source, writes to target with topic prefix (e.g., `primary.commit-log`)
- **MirrorCheckpointConnector**: Syncs consumer group offsets
- **MirrorHeartbeatConnector**: Emits heartbeats for monitoring

### Q2: What custom logic did you add to MM2?

I added offset-tracking and recovery logic to handle two edge cases that default MM2 doesn't handle well:

1. **Log Truncation Detection**: Track expected offsets per partition. If the broker's earliest offset moves past our expected offset, it means records were deleted by retention before replication — throw `LogTruncationException` to fail fast instead of silently skipping data.

2. **Topic Reset Recovery**: Detect when a topic was deleted and recreated (earliest offset = 0 while expected > 0). Instead of failing, seek to beginning and clear tracking state.

### Q3: How do you distinguish truncation from topic reset?

```
expectedOffset = 500
earliestAvailableOffset = 600  → TRUNCATION (records deleted, gap exists)
earliestAvailableOffset = 0    → RESET (topic recreated, start from 0)
```

Truncation means the earliest available offset moved FORWARD past our expected position. Reset means it went back to 0, indicating a fresh topic.

### Q4: Why not just use default MM2 behavior?

Default MM2 with `auto.offset.reset=earliest` will silently seek to the earliest available offset when it can't find the expected offset. This means:
- On truncation: it skips the missing records without warning → **silent data loss**
- On reset: it may get confused by the new topic ID → **inconsistent topic ID errors**

My solution makes both scenarios explicit and recoverable.

### Q5: Explain the event schema you used.

```json
{
  "event_id": "uuid",
  "timestamp": 1716153600,
  "op_type": "INSERT|UPDATE|DELETE",
  "key": "doc:3f2a",
  "value": { "status": "active|archived|pending|processing|completed|failed" }
}
```

This mimics a CDC (Change Data Capture) event from a database commit log.

### Q6: How does the verification script work?

`scripts/run_challenge.sh` runs 3 Docker-based scenarios:
1. **Normal**: 1000 messages, verify all replicated
2. **Truncation**: 60s retention deletes messages before MM2 starts, verify detection
3. **Reset**: Delete+recreate topic while MM2 paused, verify recovery

Uses `kafka-get-offsets.sh` to count messages and `docker compose logs` to check for custom log messages.

### Q7: What bugs did you encounter and how did you fix them?

| Bug | Fix |
|-----|-----|
| MM2 ENTRYPOINT broken (script+config as single arg) | Split into ENTRYPOINT + CMD |
| `GetOffsetShell` removed in Kafka 4.0.0 | Use `kafka-get-offsets.sh` |
| Docker Hub auth failed | Build images locally on CI runner |
| MM2 restart loop prevented `docker compose pause` | Fixed by ENTRYPOINT fix |

### Q8: How would you deploy this in production?

- Use Kubernetes with Strimzi operator for Kafka + MM2
- Store MM2 config in ConfigMaps
- Use persistent volumes for Kafka data
- Set up Prometheus + Grafana for monitoring
- Configure alerts on `LogTruncationException` in MM2 logs
- Use higher replication factors (3) and more brokers
- Set appropriate retention based on replication SLAs

### Q9: What are the limitations of this approach?

- Single-node clusters (no HA in this demo)
- 1 partition per topic (no ordering guarantees across partitions)
- Enhanced MM2 requires custom JAR build
- No exactly-once semantics (at-least-once by default)
- No automated failover to standby cluster

### Q10: How does KRaft mode differ from ZooKeeper mode?

KRaft (Kafka Raft) uses Kafka's own Raft implementation for metadata management instead of ZooKeeper. Benefits:
- Simpler deployment (no ZK cluster)
- Better scalability
- Unified security model
- Used in this solution (`KAFKA_PROCESS_ROLES: broker,controller`)

## Key Commands to Remember

```bash
# Run verification
bash scripts/run_challenge.sh

# Trigger CI
gh workflow run run-challenge.yml --ref enhanced-mm2-v2

# Check topic offsets
kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic commit-log --time latest

# View MM2 logs
docker compose logs mirrormaker2

# Manual topic operations
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic commit-log --partitions 1
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic commit-log
```

## Potential Follow-up Questions

1. **"How would you handle multi-partition topics?"** — Track expected offsets per partition independently, same logic applies per-partition.

2. **"What about exactly-once semantics?"** — Would need transactional producers on both sides + idempotent consumers. MM2 supports this with `exactly.once.source.support`.

3. **"How do you monitor replication lag?"** — Use `kafka-consumer-groups.sh` to check consumer lag, or MM2's built-in checkpoint topics.

4. **"What if the standby cluster is down?"** — MM2 buffers in internal topics; when standby comes back, replication resumes from last checkpoint.

5. **"How would you test this at scale?"** — Use Kafka's Trogdor or TLA+ specs for fault injection, increase message count to millions, add network partitions.
