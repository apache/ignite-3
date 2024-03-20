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

import static org.apache.ignite.internal.util.CursorUtils.emptyCursor;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageDestroyedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** For {@link IndexUpdateHandler} testing. */
public class IndexUpdateHandlerTest extends BaseIgniteAbstractTest {
    private static final int PARTITION_ID = 0;

    private static final int INDEX_ID = 1;

    @Test
    void testBuildIndex() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        BinaryRowAndRowId row0 = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));
        BinaryRowAndRowId row1 = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));

        indexUpdateHandler.buildIndex(INDEX_ID, Stream.of(row0, row1), row1.rowId().increment());

        verify(indexStorage).put(row0.binaryRow(), row0.rowId());
        verify(indexStorage).put(row1.binaryRow(), row1.rowId());

        verify(indexStorage.storage()).setNextRowIdToBuild(row1.rowId().increment());

        // Let's check one more batch - it will be the finishing one.
        clearInvocations(indexStorage);

        BinaryRowAndRowId row2 = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));

        indexUpdateHandler.buildIndex(INDEX_ID, Stream.of(row2), null);

        verify(indexStorage).put(row2.binaryRow(), row2.rowId());

        verify(indexStorage.storage()).setNextRowIdToBuild(null);
    }

    @Test
    void testAddToIndexesOnDestroyedIndexes() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        doThrow(StorageDestroyedException.class).when(indexStorage).put(any(), any());

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        var row = mock(BinaryRow.class);
        var rowId = new RowId(PARTITION_ID);

        assertDoesNotThrow(() -> indexUpdateHandler.addToIndexes(row, rowId, List.of(INDEX_ID, INDEX_ID + 1)));

        verify(indexStorage).put(eq(row), eq(rowId));
    }

    @Test
    void testAddToIndexesWithStorageException() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        doThrow(StorageException.class).when(indexStorage).put(any(), any());

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        assertThrows(
                StorageException.class,
                () -> indexUpdateHandler.addToIndexes(mock(BinaryRow.class), new RowId(PARTITION_ID), List.of(INDEX_ID))
        );
    }

    @Test
    void testAddToIndexOnDestroyedIndexes() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        doThrow(StorageDestroyedException.class).when(indexStorage).put(any(), any());

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        var row = mock(BinaryRow.class);
        var rowId = new RowId(PARTITION_ID);

        assertDoesNotThrow(() -> indexUpdateHandler.addToIndex(row, rowId, INDEX_ID));
        assertDoesNotThrow(() -> indexUpdateHandler.addToIndex(row, rowId, INDEX_ID + 1));

        verify(indexStorage).put(eq(row), eq(rowId));
    }

    @Test
    void testAddToIndexWithStorageException() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        doThrow(StorageException.class).when(indexStorage).put(any(), any());

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        assertThrows(StorageException.class, () -> indexUpdateHandler.addToIndex(mock(BinaryRow.class), new RowId(PARTITION_ID), INDEX_ID));
    }

    @Test
    void testBuildIndexOnDestroyedIndex() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();
        IndexStorage storage = indexStorage.storage();

        doThrow(StorageDestroyedException.class).when(indexStorage).put(any(), any());
        doThrow(StorageDestroyedException.class).when(storage).setNextRowIdToBuild(any());

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        var row = mock(BinaryRow.class);
        var rowId = new RowId(PARTITION_ID);
        var binaryRowAndRowId = new BinaryRowAndRowId(row, rowId);

        assertDoesNotThrow(() -> indexUpdateHandler.buildIndex(INDEX_ID, Stream.of(binaryRowAndRowId), rowId));
        assertDoesNotThrow(() -> indexUpdateHandler.buildIndex(INDEX_ID + 1, Stream.of(binaryRowAndRowId), rowId));

        verify(indexStorage).put(eq(row), eq(rowId));
        verify(storage).setNextRowIdToBuild(eq(rowId));
    }

    @Test
    void testBuildIndexWithStorageException() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        doThrow(StorageException.class).when(indexStorage).put(any(), any());

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        var rowId = new RowId(PARTITION_ID);
        var binaryRowAndRowId = new BinaryRowAndRowId(mock(BinaryRow.class), rowId);

        assertThrows(StorageException.class, () -> indexUpdateHandler.buildIndex(INDEX_ID, Stream.of(binaryRowAndRowId), rowId));

        IndexStorage storage = indexStorage.storage();

        doNothing().when(indexStorage).put(any(), any());
        doThrow(StorageException.class).when(storage).setNextRowIdToBuild(any());

        assertThrows(StorageException.class, () -> indexUpdateHandler.buildIndex(INDEX_ID, Stream.of(binaryRowAndRowId), rowId));
    }

    @Test
    void testGetNextRowIdToBuildIndexOnDestroyedIndex() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();
        IndexStorage storage = indexStorage.storage();

        doThrow(StorageDestroyedException.class).when(storage).getNextRowIdToBuild();

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        assertNull(indexUpdateHandler.getNextRowIdToBuildIndex(INDEX_ID));
        assertNull(indexUpdateHandler.getNextRowIdToBuildIndex(INDEX_ID + 1));

        verify(storage).getNextRowIdToBuild();
    }

    @Test
    void testGetNextRowIdToBuildIndexWithStorageException() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();
        IndexStorage storage = indexStorage.storage();

        doThrow(StorageException.class).when(storage).getNextRowIdToBuild();

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        assertThrows(StorageException.class, () -> indexUpdateHandler.getNextRowIdToBuildIndex(INDEX_ID));
    }

    @Test
    void testSetNextRowIdToBuildIndexOnDestroyedIndex() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();
        IndexStorage storage = indexStorage.storage();

        doThrow(StorageDestroyedException.class).when(storage).setNextRowIdToBuild(any());

        var rowId = new RowId(PARTITION_ID);

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        assertDoesNotThrow(() -> indexUpdateHandler.setNextRowIdToBuildIndex(INDEX_ID, rowId));
        assertDoesNotThrow(() -> indexUpdateHandler.setNextRowIdToBuildIndex(INDEX_ID + 1, rowId));

        verify(storage).setNextRowIdToBuild(eq(rowId));
    }

    @Test
    void testSetNextRowIdToBuildIndexWithStorageException() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();
        IndexStorage storage = indexStorage.storage();

        doThrow(StorageException.class).when(storage).setNextRowIdToBuild(any());

        var rowId = new RowId(PARTITION_ID);

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        assertThrows(StorageException.class, () -> indexUpdateHandler.setNextRowIdToBuildIndex(INDEX_ID, rowId));
    }

    @Test
    void testTryRemoveFromIndexesOnDestroyedIndex() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();
        ColumnsExtractor columnsExtractor = indexStorage.indexRowResolver();

        doThrow(StorageDestroyedException.class).when(indexStorage).remove(any(), any());
        when(columnsExtractor.extractColumns(any())).thenReturn(mock(BinaryTuple.class));

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        var row = mock(BinaryRow.class);
        var rowId = new RowId(PARTITION_ID);

        assertDoesNotThrow(() -> indexUpdateHandler.tryRemoveFromIndexes(row, rowId, emptyCursor(), List.of(INDEX_ID, INDEX_ID + 1)));

        verify(indexStorage).remove(eq(row), eq(rowId));
    }

    @Test
    void testTryRemoveFromIndexesWithStorageException() {
        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();
        ColumnsExtractor columnsExtractor = indexStorage.indexRowResolver();

        doThrow(StorageException.class).when(indexStorage).remove(any(), any());
        when(columnsExtractor.extractColumns(any())).thenReturn(mock(BinaryTuple.class));

        var indexUpdateHandler = new IndexUpdateHandler(indexStoragesSupplier(Map.of(INDEX_ID, indexStorage)));

        var row = mock(BinaryRow.class);
        var rowId = new RowId(PARTITION_ID);

        assertThrows(
                StorageException.class,
                () -> indexUpdateHandler.tryRemoveFromIndexes(row, rowId, emptyCursor(), List.of(INDEX_ID, INDEX_ID + 1))
        );
    }

    private static TableSchemaAwareIndexStorage createIndexStorage() {
        TableSchemaAwareIndexStorage indexStorage = mock(TableSchemaAwareIndexStorage.class);

        IndexStorage storage = mock(IndexStorage.class);

        when(indexStorage.storage()).thenReturn(storage);

        ColumnsExtractor columnsExtractor = mock(ColumnsExtractor.class);

        when(indexStorage.indexRowResolver()).thenReturn(columnsExtractor);

        return indexStorage;
    }

    private static TableIndexStoragesSupplier indexStoragesSupplier(Map<Integer, TableSchemaAwareIndexStorage> indexStorageById) {
        return () -> indexStorageById;
    }
}
