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

package org.apache.ignite.internal.table.distributed.index;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.junit.jupiter.api.Test;

/**
 * For {@link IndexUpdateHandler} testing.
 */
public class IndexUpdateHandlerTest {
    private static final int PARTITION_ID = 0;

    @Test
    void testBuildIndex() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        UUID indexId = UUID.randomUUID();

        TableIndexStoragesSupplier indexes = mock(TableIndexStoragesSupplier.class);

        when(indexes.get()).thenReturn(Map.of(indexId, indexStorage));

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        BuildIndexRow buildIndexRow0 = new BuildIndexRow(new RowId(PARTITION_ID), mock(BinaryRow.class));
        BuildIndexRow buildIndexRow1 = new BuildIndexRow(new RowId(PARTITION_ID), mock(BinaryRow.class));

        indexUpdateHandler.buildIndex(indexId, Stream.of(buildIndexRow0, buildIndexRow1), buildIndexRow1.getRowId().increment());

        verify(indexes).addIndexToWaitIfAbsent(indexId);

        verify(indexStorage).put(buildIndexRow0.getBinaryRow(), buildIndexRow0.getRowId());
        verify(indexStorage).put(buildIndexRow1.getBinaryRow(), buildIndexRow1.getRowId());

        verify(indexStorage.storage()).setNextRowIdToBuild(buildIndexRow1.getRowId().increment());

        // Let's check one more batch - it will be the finishing one.
        clearInvocations(indexes, indexStorage);

        BuildIndexRow buildIndexRow2 = new BuildIndexRow(new RowId(PARTITION_ID), mock(BinaryRow.class));

        indexUpdateHandler.buildIndex(indexId, Stream.of(buildIndexRow2), null);

        verify(indexes).addIndexToWaitIfAbsent(indexId);

        verify(indexStorage).put(buildIndexRow2.getBinaryRow(), buildIndexRow2.getRowId());

        verify(indexStorage.storage()).setNextRowIdToBuild(null);
    }

    private static TableSchemaAwareIndexStorage createIndexStorage() {
        TableSchemaAwareIndexStorage indexStorage = mock(TableSchemaAwareIndexStorage.class);

        IndexStorage storage = mock(IndexStorage.class);

        when(indexStorage.storage()).thenReturn(storage);

        return indexStorage;
    }
}
