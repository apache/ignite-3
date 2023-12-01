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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Index update handler.
 */
public class IndexUpdateHandler {
    private final TableIndexStoragesSupplier indexes;

    /**
     * Constructor.
     *
     * @param indexes Indexes supplier.
     */
    public IndexUpdateHandler(TableIndexStoragesSupplier indexes) {
        this.indexes = indexes;
    }

    /**
     * Adds a binary row to the indexes, if the tombstone then skips such operation.
     *
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.</p>
     *
     * @param binaryRow Binary row to insert.
     * @param rowId Row ID.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    public void addToIndexes(
            @Nullable BinaryRow binaryRow,
            RowId rowId,
            // TODO: IGNITE-18595 We need to know the indexes for a full rebalance, i.e. null must go
            @Nullable List<Integer> indexIds
    ) {
        assert indexIds == null || !indexIds.isEmpty() : indexIds;

        if (binaryRow == null) { // skip removes
            return;
        }

        Map<Integer, TableSchemaAwareIndexStorage> indexStorageByIndexId = indexStorageByIndexId();

        if (indexIds == null) {
            indexStorageByIndexId.values().forEach(indexStorage -> indexStorage.put(binaryRow, rowId));
        } else {
            for (Integer indexId : indexIds) {
                TableSchemaAwareIndexStorage indexStorage = indexStorageByIndexId.get(indexId);

                assert indexStorage != null : indexId;

                indexStorage.put(binaryRow, rowId);
            }
        }
    }

    /**
     * Tries removing indexed row from every index.
     * Removes the row only if no previous value's index matches index of the row to remove, because if it matches, then the index
     * might still be in use.
     *
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.</p>
     *
     * @param rowToRemove Row to remove from indexes.
     * @param rowId Row id.
     * @param previousValues Cursor with previous version of the row.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    public void tryRemoveFromIndexes(
            BinaryRow rowToRemove,
            RowId rowId,
            Cursor<ReadResult> previousValues,
            @Nullable List<Integer> indexIds
    ) {
        assert indexIds == null || !indexIds.isEmpty() : indexIds;

        TableSchemaAwareIndexStorage[] indexes = indexStoragesSnapshot(indexIds);

        ByteBuffer[] indexValues = new ByteBuffer[indexes.length];

        // Precalculate value for every index.
        for (int i = 0; i < indexes.length; i++) {
            TableSchemaAwareIndexStorage index = indexes[i];

            indexValues[i] = index.indexRowResolver().extractColumns(rowToRemove).byteBuffer();
        }

        while (previousValues.hasNext()) {
            ReadResult previousVersion = previousValues.next();

            BinaryRow previousRow = previousVersion.binaryRow();

            // No point in cleaning up indexes for tombstone, they should not exist.
            if (previousRow != null) {
                for (int i = 0; i < indexes.length; i++) {
                    TableSchemaAwareIndexStorage index = indexes[i];

                    if (index == null) {
                        continue;
                    }

                    // If any of the previous versions' index value equals the index value of
                    // the row to remove, then we can't remove that index as it can still be used.
                    BinaryTuple previousRowIndex = index.indexRowResolver().extractColumns(previousRow);

                    if (indexValues[i].equals(previousRowIndex.byteBuffer())) {
                        indexes[i] = null;
                    }
                }
            }
        }

        for (TableSchemaAwareIndexStorage index : indexes) {
            if (index != null) {
                index.remove(rowToRemove, rowId);
            }
        }
    }

    /**
     * Builds an exist index for all versions of a row.
     *
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.
     *
     * @param indexId Index ID.
     * @param rowStream Stream of rows to build the index without tombstones.
     * @param nextRowIdToBuild Row ID for which the index needs to be build next, {@code null} means that the index is build.
     */
    public void buildIndex(int indexId, Stream<BinaryRowAndRowId> rowStream, @Nullable RowId nextRowIdToBuild) {
        // TODO: IGNITE-19082 Need another way to wait for index creation
        indexes.addIndexToWaitIfAbsent(indexId);

        TableSchemaAwareIndexStorage index = indexes.get().get(indexId);

        rowStream.forEach(binaryRowAndRowId -> {
            assert binaryRowAndRowId.binaryRow() != null;

            index.put(binaryRowAndRowId.binaryRow(), binaryRowAndRowId.rowId());
        });

        index.storage().setNextRowIdToBuild(nextRowIdToBuild);
    }

    /**
     * Waits for indexes to be created.
     */
    // TODO: IGNITE-19513 Fix it, we should have already waited for the indexes to be created
    public void waitIndexes() {
        indexes.get();
    }

    /**
     * Waits for the specific index to be created.
     */
    public void waitForIndex(int indexId) {
        indexes.addIndexToWaitIfAbsent(indexId);

        waitIndexes();
    }

    private Map<Integer, TableSchemaAwareIndexStorage> indexStorageByIndexId() {
        Map<Integer, TableSchemaAwareIndexStorage> indexes = this.indexes.get();

        assert !indexes.isEmpty();

        return indexes;
    }

    private TableSchemaAwareIndexStorage[] indexStoragesSnapshot(@Nullable List<Integer> indexIds) {
        Map<Integer, TableSchemaAwareIndexStorage> indexStorageByIndexId = indexStorageByIndexId();

        if (indexIds == null) {
            return indexStorageByIndexId.values().toArray(TableSchemaAwareIndexStorage[]::new);
        }

        TableSchemaAwareIndexStorage[] indexes = new TableSchemaAwareIndexStorage[indexIds.size()];

        for (int i = 0; i < indexIds.size(); i++) {
            Integer indexId = indexIds.get(i);

            TableSchemaAwareIndexStorage indexStorage = indexStorageByIndexId.get(indexId);

            assert indexStorage != null : indexId;

            indexes[i] = indexStorage;
        }

        return indexes;
    }
}
