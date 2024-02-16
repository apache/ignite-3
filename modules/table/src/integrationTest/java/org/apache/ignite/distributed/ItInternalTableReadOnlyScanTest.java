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

package org.apache.ignite.distributed;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link InternalTable#scan(int, org.apache.ignite.internal.tx.InternalTransaction)}.
 */
@ExtendWith(MockitoExtension.class)
public class ItInternalTableReadOnlyScanTest extends ItAbstractInternalTableScanTest {
    private static final HybridTimestampTracker HYBRID_TIMESTAMP_TRACKER = new HybridTimestampTracker();

    @Override
    protected Publisher<BinaryRow> scan(int part, @Nullable InternalTransaction tx) {
        requireNonNull(tx);

        return internalTbl.scan(part, tx.id(), internalTbl.CLOCK.now(), mock(ClusterNode.class), tx.coordinatorId());
    }

    // TODO: IGNITE-17666 Use super test as is.
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17666")
    @Override
    @Test
    public void testExceptionRowScanCursorHasNext() throws Exception {
        super.testExceptionRowScanCursorHasNext();
    }

    @Override
    protected InternalTransaction startTx() {
        return internalTbl.txManager().begin(HYBRID_TIMESTAMP_TRACKER, true);
    }

    @Override
    protected void validateTxAbortedState(InternalTransaction tx) {
        // noop since we do not store state for readonly transactions.
    }

    /**
     * Checks that {@link IllegalArgumentException} is thrown in case of invalid partition.
     */
    @Test
    public void testInvalidPartitionParameterScan() {
        assertThrows(
                IllegalArgumentException.class,
                () -> scan(-1, startTx())
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> scan(1, startTx())
        );
    }
}
