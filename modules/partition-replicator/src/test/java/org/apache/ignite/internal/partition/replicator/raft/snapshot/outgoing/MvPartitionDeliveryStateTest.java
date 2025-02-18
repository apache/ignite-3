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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MvPartitionDeliveryStateTest extends BaseIgniteAbstractTest {
    private static final int PARTITION_ID = 1;

    @Test
    void emptyStateIsExhausted() {
        var state = new MvPartitionDeliveryState(List.of());

        assertThat(state.hasIterationStarted(), is(false));
        assertThat(state.isExhausted(), is(true));
    }

    @Test
    void correctlyIteratesOverAllStorages(
            @Mock PartitionMvStorageAccess storage1,
            @Mock PartitionMvStorageAccess storage2,
            @Mock PartitionMvStorageAccess storage3
    ) {
        List<PartitionMvStorageAccess> storages = List.of(storage1, storage2, storage3);

        var rowIdsByTable = new HashMap<Integer, List<RowId>>();

        for (int i = 0; i < storages.size(); i++) {
            List<RowId> rowIds = generateRowIds(3);

            rowIdsByTable.put(i, rowIds);

            PartitionMvStorageAccess storage = storages.get(i);

            when(storage.partitionId()).thenReturn(PARTITION_ID);
            when(storage.tableId()).thenReturn(i);

            when(storage.closestRowId(any()))
                    .thenReturn(rowIds.get(0))
                    .thenReturn(rowIds.get(1))
                    .thenReturn(rowIds.get(2))
                    .thenReturn(null);
        }

        var state = new MvPartitionDeliveryState(storages);

        assertThat(state.hasIterationStarted(), is(false));
        assertThat(state.isExhausted(), is(false));

        for (int i = 0; i < storages.size(); i++) {
            state.advance();

            assertThat(state.hasIterationStarted(), is(true));
            assertThat(state.isExhausted(), is(false));
            assertThat(state.currentTableId(), is(i));
            assertThat(state.currentRowId(), is(rowIdsByTable.get(i).get(0)));

            state.advance();

            assertThat(state.hasIterationStarted(), is(true));
            assertThat(state.isExhausted(), is(false));
            assertThat(state.currentTableId(), is(i));
            assertThat(state.currentRowId(), is(rowIdsByTable.get(i).get(1)));

            state.advance();

            assertThat(state.hasIterationStarted(), is(true));
            assertThat(state.isExhausted(), is(false));
            assertThat(state.currentTableId(), is(i));
            assertThat(state.currentRowId(), is(rowIdsByTable.get(i).get(2)));
        }

        state.advance();

        assertThat(state.hasIterationStarted(), is(true));
        assertThat(state.isExhausted(), is(true));
    }

    @Test
    void handlesRowIdBoundaries(
            @Mock PartitionMvStorageAccess storage1,
            @Mock PartitionMvStorageAccess storage2
    ) {
        // We are using two storages to test that table IDs are correctly iterated as well.
        when(storage1.partitionId()).thenReturn(PARTITION_ID);
        when(storage1.tableId()).thenReturn(1);
        when(storage2.partitionId()).thenReturn(PARTITION_ID);
        when(storage2.tableId()).thenReturn(2);

        RowId lowestRowId = RowId.lowestRowId(PARTITION_ID);
        RowId highestRowId = RowId.highestRowId(PARTITION_ID);

        when(storage1.closestRowId(lowestRowId)).thenReturn(lowestRowId);
        when(storage1.closestRowId(lowestRowId.increment())).thenReturn(highestRowId);

        when(storage2.closestRowId(lowestRowId)).thenReturn(lowestRowId);
        when(storage2.closestRowId(lowestRowId.increment())).thenReturn(highestRowId);

        var state = new MvPartitionDeliveryState(List.of(storage1, storage2));

        assertThat(state.hasIterationStarted(), is(false));
        assertThat(state.isExhausted(), is(false));

        IntStream.rangeClosed(1, 2).forEach(i -> {
            state.advance();

            assertThat(state.hasIterationStarted(), is(true));
            assertThat(state.isExhausted(), is(false));

            assertThat(state.currentTableId(), is(i));
            assertThat(state.currentRowId(), is(lowestRowId));

            state.advance();

            assertThat(state.hasIterationStarted(), is(true));
            assertThat(state.isExhausted(), is(false));
            assertThat(state.currentTableId(), is(i));
            assertThat(state.currentRowId(), is(highestRowId));
        });

        state.advance();

        assertThat(state.hasIterationStarted(), is(true));
        assertThat(state.isExhausted(), is(true));
    }

    private static List<RowId> generateRowIds(int count) {
        return IntStream.range(0, count).mapToObj(RowId::new).sorted().collect(toList());
    }
}
