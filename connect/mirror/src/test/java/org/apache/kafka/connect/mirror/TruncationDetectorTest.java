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

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TruncationDetectorTest {

    private final TopicPartition tp = new TopicPartition("test-topic", 0);

    @Test
    public void testNoTruncationWhenEarliestIsZero() {
        TruncationDetector detector = new TruncationDetector();
        detector.updateExpectedOffset(tp, 99L);
        assertDoesNotThrow(() -> detector.checkForTruncation(tp, 0L));
    }

    @Test
    public void testTruncationDetectedWhenEarliestGreaterThanExpected() {
        TruncationDetector detector = new TruncationDetector();
        detector.updateExpectedOffset(tp, 99L);
        LogTruncationException ex = assertThrows(LogTruncationException.class,
                () -> detector.checkForTruncation(tp, 500L));
        assertEquals(tp, ex.topicPartition());
        assertEquals(100L, ex.expectedOffset());
        assertEquals(500L, ex.earliestAvailableOffset());
    }

    @Test
    public void testTruncationDetectedWhenEarliestEqualsExpected() {
        TruncationDetector detector = new TruncationDetector();
        detector.updateExpectedOffset(tp, 499L);
        LogTruncationException ex = assertThrows(LogTruncationException.class,
                () -> detector.checkForTruncation(tp, 500L));
        assertEquals(tp, ex.topicPartition());
        assertEquals(500L, ex.expectedOffset());
        assertEquals(500L, ex.earliestAvailableOffset());
    }

    @Test
    public void testNoTruncationWhenNotTracked() {
        TruncationDetector detector = new TruncationDetector();
        assertDoesNotThrow(() -> detector.checkForTruncation(tp, 500L));
    }

    @Test
    public void testGetExpectedOffsetReturnsNegativeWhenNotTracked() {
        TruncationDetector detector = new TruncationDetector();
        assertEquals(-1L, detector.getExpectedOffset(tp));
    }

    @Test
    public void testResetPartitionClearsTracking() {
        TruncationDetector detector = new TruncationDetector();
        detector.updateExpectedOffset(tp, 99L);
        assertEquals(100L, detector.getExpectedOffset(tp));
        detector.resetPartition(tp);
        assertEquals(-1L, detector.getExpectedOffset(tp));
    }

    @Test
    public void testResetAllClearsAllTracking() {
        TruncationDetector detector = new TruncationDetector();
        TopicPartition tp2 = new TopicPartition("test-topic", 1);
        detector.updateExpectedOffset(tp, 99L);
        detector.updateExpectedOffset(tp2, 199L);
        detector.resetAll();
        assertEquals(-1L, detector.getExpectedOffset(tp));
        assertEquals(-1L, detector.getExpectedOffset(tp2));
    }
}
