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

package org.apache.ignite.internal.table.distributed.replicator;

import static org.apache.ignite.internal.table.distributed.replicator.ReplicatorUtils.beginRwTxTs;
import static org.apache.ignite.internal.table.distributed.replicator.ReplicatorUtils.rwTxActiveCatalogVersion;
import static org.apache.ignite.internal.tx.TransactionIds.transactionId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** For {@link ReplicatorUtils} testing. */
public class ReplicatorUtilsTest extends IgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    @Test
    void testBeginRwTxTs() {
        HybridTimestamp beginTs = clock.now();

        UUID txId = transactionId(beginTs, 10);

        assertEquals(beginTs, beginRwTxTs(readWriteReplicaRequest(txId)));
    }

    @Test
    void testRwTxActiveCatalogVersion() {
        HybridTimestamp beginTs = clock.now();

        UUID txId = transactionId(beginTs, 10);

        CatalogService catalogService = mock(CatalogService.class);

        when(catalogService.activeCatalogVersion(anyLong())).thenReturn(666);

        assertEquals(666, rwTxActiveCatalogVersion(catalogService, readWriteReplicaRequest(txId)));

        verify(catalogService).activeCatalogVersion(eq(beginTs.longValue()));
    }

    private static ReadWriteReplicaRequest readWriteReplicaRequest(UUID txId) {
        ReadWriteReplicaRequest request = mock(ReadWriteReplicaRequest.class);

        when(request.transactionId()).thenReturn(txId);

        return request;
    }
}
