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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionAccessImplTest {
    @Mock
    private MvPartitionStorage partitionStorage;

    @Mock
    private TxStateStorage txStateStorage;

    private PartitionAccessImpl access;

    private final PartitionKey key = new PartitionKey(UUID.randomUUID(), 1);

    @BeforeEach
    void createTestInstance() {
        access = new PartitionAccessImpl(key, partitionStorage, txStateStorage);
    }

    @Test
    void returnsProvidedKey() {
        assertThat(access.key(), is(key));
    }

    @Test
    void persistedIndexDelegatesToStorage() {
        when(partitionStorage.persistedIndex()).thenReturn(42L);

        assertThat(access.persistedIndex(), is(42L));
    }

    @Test
    void minRowIdDelegatesToStorage() {
        RowId argRowId = new RowId(1);
        RowId resultRowId = new RowId(1);

        when(partitionStorage.closestRowId(any())).thenReturn(resultRowId);

        assertThat(access.closestRowId(argRowId), is(resultRowId));
    }

    @Test
    void returnsRowVersionsFromStorage() {
        ReadResult result1 = mock(ReadResult.class);
        ReadResult result2 = mock(ReadResult.class);

        when(partitionStorage.scanVersions(any()))
                .thenReturn(Cursor.fromIterator(List.of(result1, result2).iterator()));

        List<ReadResult> versions = access.rowVersions(new RowId(1));
        assertThat(versions, is(equalTo(List.of(result1, result2))));
    }

    @Test
    void delegatesTxDataScanToTxStorage() {
        @SuppressWarnings("unchecked") Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = mock(Cursor.class);

        when(txStateStorage.scan()).thenReturn(cursor);

        assertThat(access.scanTxData(), is(cursor));
    }
}
