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
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * For {@link IndexUpdateHandler} testing.
 */
public class IndexUpdateHandlerTest extends BaseIgniteAbstractTest {
    private static final int PARTITION_ID = 0;

    @Test
    void testBuildIndex() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        int indexId = 1;

        TableIndexStoragesSupplier indexes = mock(TableIndexStoragesSupplier.class);

        when(indexes.get()).thenReturn(Map.of(indexId, indexStorage));

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        BinaryRowAndRowId row0 = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));
        BinaryRowAndRowId row1 = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));

        indexUpdateHandler.buildIndex(indexId, Stream.of(row0, row1), row1.rowId().increment());

        verify(indexes).addIndexToWaitIfAbsent(indexId);

        verify(indexStorage).put(row0.binaryRow(), row0.rowId());
        verify(indexStorage).put(row1.binaryRow(), row1.rowId());

        verify(indexStorage.storage()).setNextRowIdToBuild(row1.rowId().increment());

        // Let's check one more batch - it will be the finishing one.
        clearInvocations(indexes, indexStorage);

        BinaryRowAndRowId row2 = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));

        indexUpdateHandler.buildIndex(indexId, Stream.of(row2), null);

        verify(indexes).addIndexToWaitIfAbsent(indexId);

        verify(indexStorage).put(row2.binaryRow(), row2.rowId());

        verify(indexStorage.storage()).setNextRowIdToBuild(null);
    }

    private static TableSchemaAwareIndexStorage createIndexStorage() {
        TableSchemaAwareIndexStorage indexStorage = mock(TableSchemaAwareIndexStorage.class);

        IndexStorage storage = mock(IndexStorage.class);

        when(indexStorage.storage()).thenReturn(storage);

        return indexStorage;
    }
}
