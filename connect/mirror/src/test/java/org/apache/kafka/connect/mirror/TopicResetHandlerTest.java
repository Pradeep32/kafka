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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TopicResetHandlerTest {

    private final TopicPartition tp = new TopicPartition("test-topic", 0);

    @Test
    public void testIsTopicResetWhenEarliestIsZeroAndExpectedPositive() {
        TopicResetHandler handler = new TopicResetHandler();
        assertTrue(handler.isTopicReset(tp, 0L, 200L));
    }

    @Test
    public void testIsNotTopicResetWhenEarliestPositive() {
        TopicResetHandler handler = new TopicResetHandler();
        assertFalse(handler.isTopicReset(tp, 500L, 200L));
    }

    @Test
    public void testIsNotTopicResetWhenExpectedIsZero() {
        TopicResetHandler handler = new TopicResetHandler();
        assertFalse(handler.isTopicReset(tp, 0L, 0L));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testHandleOffsetOutOfRangeSeeksToBeginning() {
        TopicResetHandler handler = new TopicResetHandler();
        TruncationDetector truncationDetector = new TruncationDetector();
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);

        Map<TopicPartition, Long> offsetMap = Map.of(tp, 200L);
        OffsetOutOfRangeException exception = new OffsetOutOfRangeException(offsetMap);

        boolean recovered = handler.handleOffsetOutOfRange(consumer, exception, truncationDetector);
        assertTrue(recovered);
        verify(consumer).seekToBeginning(Set.of(tp));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResubscribeFromBeginning() {
        TopicResetHandler handler = new TopicResetHandler();
        TruncationDetector truncationDetector = new TruncationDetector();
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);

        truncationDetector.updateExpectedOffset(tp, 99L);
        handler.resubscribeFromBeginning(consumer, Collections.singleton(tp), truncationDetector);

        verify(consumer).seekToBeginning(Collections.singleton(tp));
        assertTrue(truncationDetector.getExpectedOffset(tp) < 0);
    }
}
