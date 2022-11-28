/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.persistence;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * For {@link PartitionProcessingCounterMap} testing.
 */
public class PartitionProcessingCounterMapTest {
    @Test
    void test() {
        PartitionProcessingCounterMap processingCounterMap = new PartitionProcessingCounterMap();

        GroupPartitionId groupPartitionId = new GroupPartitionId(0, 0);

        assertNull(processingCounterMap.getProcessedPartitionFuture(groupPartitionId));

        processingCounterMap.incrementPartitionProcessingCounter(groupPartitionId);

        CompletableFuture<Void> processedPartitionFuture0 = processingCounterMap.getProcessedPartitionFuture(groupPartitionId);

        assertNotNull(processedPartitionFuture0);
        assertFalse(processedPartitionFuture0.isDone());

        processingCounterMap.incrementPartitionProcessingCounter(groupPartitionId);

        assertSame(processedPartitionFuture0, processingCounterMap.getProcessedPartitionFuture(groupPartitionId));
        assertFalse(processedPartitionFuture0.isDone());

        processingCounterMap.decrementPartitionProcessingCounter(groupPartitionId);

        assertSame(processedPartitionFuture0, processingCounterMap.getProcessedPartitionFuture(groupPartitionId));
        assertFalse(processedPartitionFuture0.isDone());

        processingCounterMap.decrementPartitionProcessingCounter(groupPartitionId);

        assertNull(processingCounterMap.getProcessedPartitionFuture(groupPartitionId));
        assertTrue(processedPartitionFuture0.isDone());

        // Let's check the reprocessing of the partition.

        processingCounterMap.incrementPartitionProcessingCounter(groupPartitionId);

        CompletableFuture<Void> processedPartitionFuture1 = processingCounterMap.getProcessedPartitionFuture(groupPartitionId);

        assertNotNull(processedPartitionFuture1);
        assertFalse(processedPartitionFuture1.isDone());
        assertNotSame(processedPartitionFuture0, processedPartitionFuture1);

        processingCounterMap.decrementPartitionProcessingCounter(groupPartitionId);

        assertNull(processingCounterMap.getProcessedPartitionFuture(groupPartitionId));
        assertTrue(processedPartitionFuture1.isDone());
    }
}
