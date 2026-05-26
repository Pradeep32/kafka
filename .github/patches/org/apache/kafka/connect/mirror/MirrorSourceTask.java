/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/** Replicates a set of topic-partitions with truncation detection and topic reset handling. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;
    private final TruncationDetector truncationDetector = new TruncationDetector();
    private final TopicResetHandler topicResetHandler = new TopicResetHandler();

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig("replication-consumer"));
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);

        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        if (offsetSyncWriter != null) {
            offsetSyncWriter.promoteDelayedOffsetSyncs();
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway.");
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }

    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));

                // Track expected offset for truncation/reset detection
                TopicPartition sourceTp = new TopicPartition(record.topic(), record.partition());
                truncationDetector.updateExpectedOffset(sourceTp, record.offset());
            }
            if (sourceRecords.isEmpty()) {
                // Check for topic reset on empty poll: if consumer position is 0 but
                // we expected a higher offset, the topic was likely deleted and recreated
                checkForTopicReset();
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (WakeupException e) {
            return null;
        } catch (OffsetOutOfRangeException e) {
            // Use TopicResetHandler to recover (it will throw LogTruncationException if truncation detected)
            log.warn("OffsetOutOfRangeException in poll: {}", e.offsetOutOfRangePartitions());
            boolean recovered = topicResetHandler.handleOffsetOutOfRange(consumer, e, truncationDetector);
            if (!recovered) {
                throw e;
            }
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e)  {
            log.error("Failure during poll.", e);
            throw e;
        } finally {
            consumerAccess.release();
        }
    }

    private void checkForTopicReset() {
        for (TopicPartition tp : consumer.assignment()) {
            long expected = truncationDetector.getExpectedOffset(tp);
            if (expected <= 0) continue;
            try {
                Map<TopicPartition, Long> beginningOffsets =
                        consumer.beginningOffsets(Collections.singleton(tp));
                long earliest = beginningOffsets.getOrDefault(tp, 0L);
                Map<TopicPartition, Long> endOffsets =
                        consumer.endOffsets(Collections.singleton(tp));
                long end = endOffsets.getOrDefault(tp, 0L);
                if (topicResetHandler.isTopicReset(tp, earliest, expected, end)) {
                    log.warn("TOPIC RESET DETECTED for {}: earliest={}, end={}, expected={}. "
                            + "Resubscribing from beginning.", tp, earliest, end, expected);
                    topicResetHandler.resubscribeFromBeginning(consumer,
                            Collections.singleton(tp), truncationDetector);
                }
            } catch (Exception ex) {
                log.debug("Could not check topic reset for {}: {}", tp, ex.getMessage());
            }
        }
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out during transformation) -- can't sync offsets for {}.", record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.", topicPartitionOffsets.values().stream()
                .filter(this::isUncommitted).count());

        // Check for topic reset before seeking: if earliest offset is 0, end > 0,
        // but we have committed offsets > 0, the topic was likely deleted and recreated.
        // Cold-state guard: only check when committedOffset > 0 (not -1 or 0 from cold start).
        // Offset store note: after reset, the next poll() returns records from offset 0.
        // When those records are committed via commitRecord(), the offset store is naturally
        // updated with the new offsets. On subsequent restarts, loadOffsets() returns the
        // correct post-reset offsets. The only gap is if MM2 crashes before the first
        // post-reset commit — in that case, this check runs again on restart.
        Set<TopicPartition> committedPartitions = topicPartitionOffsets.entrySet().stream()
                .filter(e -> !isUncommitted(e.getValue()) && e.getValue() > 0)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        if (!committedPartitions.isEmpty()) {
            try {
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(committedPartitions);
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(committedPartitions);
                for (TopicPartition tp : committedPartitions) {
                    long committedOffset = topicPartitionOffsets.get(tp);
                    long earliestOffset = beginningOffsets.getOrDefault(tp, 0L);
                    long endOffset = endOffsets.getOrDefault(tp, 0L);
                    if (topicResetHandler.isTopicReset(tp, earliestOffset, committedOffset + 1, endOffset)) {
                        log.warn("TOPIC RESET DETECTED for {}: committed offset={}, earliest={}, end={}. "
                                + "Topic was likely deleted and recreated. Resubscribing from beginning.",
                                tp, committedOffset, earliestOffset, endOffset);
                        topicResetHandler.resubscribeFromBeginning(consumer,
                                Collections.singleton(tp), truncationDetector);
                        topicPartitionOffsets.put(tp, -1L);
                    }
                }
            } catch (Exception ex) {
                log.warn("Topic reset check skipped: {}", ex.getMessage());
            }
        }

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (isUncommitted(offset)) {
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }
            long nextOffsetToCommittedOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffsetToCommittedOffset, topicPartition);
            consumer.seek(topicPartition, nextOffsetToCommittedOffset);
            // Seed truncation detector with committed offset
            truncationDetector.updateExpectedOffset(topicPartition, offset);
        });

        // Startup truncation check: if earliest offset > committed offset, truncation occurred while we were down
        Set<TopicPartition> stillCommitted = topicPartitionOffsets.entrySet().stream()
                .filter(e -> !isUncommitted(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        if (!stillCommitted.isEmpty()) {
            try {
                Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(stillCommitted);
                for (TopicPartition tp : stillCommitted) {
                    long earliest = earliestOffsets.getOrDefault(tp, 0L);
                    log.info("Startup truncation check for {}: committed={}, earliest={}",
                            tp, topicPartitionOffsets.get(tp), earliest);
                    truncationDetector.checkForTruncation(tp, earliest);
                }
            } catch (LogTruncationException lte) {
                throw lte; // fail-fast
            } catch (Exception ex) {
                log.warn("Startup truncation check skipped: {}", ex.getMessage());
            }
        }
    }

    // visible for testing
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return bytes.length;
        }
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }
}
