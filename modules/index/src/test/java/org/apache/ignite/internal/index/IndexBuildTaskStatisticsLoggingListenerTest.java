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

package org.apache.ignite.internal.index;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.tx.TxState;
import org.junit.jupiter.api.Test;

class IndexBuildTaskStatisticsLoggingListenerTest {

    private final IndexBuildTaskId taskId = new IndexBuildTaskId(1, 1, 1, 1);

    private final IndexBuildTaskStatisticsLoggingListener listener = new IndexBuildTaskStatisticsLoggingListener(
            taskId, false
    );

    @Test
    void testOnIndexBuildSuccessPasses() {
        listener.onIndexBuildStarted();

        assertDoesNotThrow(listener::onIndexBuildSuccess);
    }

    @Test
    void testOnIndexBuildFailurePasses() {
        listener.onIndexBuildStarted();

        assertDoesNotThrow(() -> listener.onIndexBuildFailure(new RuntimeException("Index build exception")));
    }

    @Test
    void testOnIndexBuildStartedSetsStartTime() {
        listener.onIndexBuildStarted();

        assertTrue(listener.startTime().get() > 0);
    }

    @Test
    void testOnBatchProcessedAccumulatesRowCount() {
        listener.onBatchProcessed(10);
        listener.onBatchProcessed(5);

        assertEquals(15, listener.rowIndexedCount().get());
    }

    @Test
    void testOnRaftCallSuccessAndFailureAccumulateCounts() {
        listener.onRaftCallSuccess();
        listener.onRaftCallSuccess();
        listener.onRaftCallFailure();

        assertEquals(2, listener.successfulRaftCallCount().get());
        assertEquals(1, listener.failedRaftCallCount().get());
    }

    @Test
    void testOnWriteIntentResolvedAccumulatesCounts() {
        listener.onWriteIntentResolved(TxState.COMMITTED);
        listener.onWriteIntentResolved(TxState.COMMITTED);
        listener.onWriteIntentResolved(TxState.ABANDONED);

        assertEquals(2, listener.resolvedWriteIntentCount().size());
        assertEquals(2, listener.resolvedWriteIntentCount().get(TxState.COMMITTED).get());
        assertEquals(1, listener.resolvedWriteIntentCount().get(TxState.ABANDONED).get());
    }

    @Test
    void testExceptionOnStartTimeHasNotBeenSet() {
        assertThrows(IllegalStateException.class, listener::onIndexBuildSuccess);
    }
}
