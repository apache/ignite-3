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
import java.util.ArrayList;
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
import org.apache.ignite.internal.storage.StorageDestroyedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/** Index update handler. */
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
    public void addToIndexes(@Nullable BinaryRow binaryRow, RowId rowId, @Nullable List<Integer> indexIds) {
        assert indexIds == null || !indexIds.isEmpty() : indexIds;

        if (binaryRow == null) { // skip removes
            return;
        }

        for (TableSchemaAwareIndexStorage index : indexes(indexIds)) {
            putToIndex(index, binaryRow, rowId);
        }
    }

    /**
     * Adds a binary row to the index, if it's a tombstone then skips such operation.
     *
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.</p>
     *
     * @param binaryRow Binary row to insert.
     * @param rowId Row ID.
     * @param indexId ID of index that will need to be updated.
     */
    public void addToIndex(@Nullable BinaryRow binaryRow, RowId rowId, int indexId) {
        if (binaryRow == null) { // skip removes
            return;
        }

        TableSchemaAwareIndexStorage indexStorage = indexStorageById().get(indexId);

        if (indexStorage != null) {
            putToIndex(indexStorage, binaryRow, rowId);
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
        TableSchemaAwareIndexStorage[] indexes = indexesSnapshot(indexIds);

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
                removeFromIndex(index, rowToRemove, rowId);
            }
        }
    }

    /**
     * Builds an exist index for all versions of a row.
     *
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.</p>
     *
     * @param indexId Index ID.
     * @param rowStream Stream of rows to build the index without tombstones.
     * @param nextRowIdToBuild Row ID for which the index needs to be build next, {@code null} means that the index is build.
     */
    public void buildIndex(int indexId, Stream<BinaryRowAndRowId> rowStream, @Nullable RowId nextRowIdToBuild) {
        TableSchemaAwareIndexStorage index = indexStorageById().get(indexId);

        // We assume that if the index is missing in indexStorageById, then it has begun to be destroyed and we do not need it.
        if (index == null) {
            return;
        }

        rowStream.forEach(binaryRowAndRowId -> {
            BinaryRow binaryRow = binaryRowAndRowId.binaryRow();

            assert binaryRow != null : "indexId=" + indexId + ", rowId=" + binaryRowAndRowId.rowId();

            putToIndex(index, binaryRow, binaryRowAndRowId.rowId());
        });

        setNextRowIdToBuildToIndex(index, nextRowIdToBuild);
    }

    private Iterable<TableSchemaAwareIndexStorage> indexes(@Nullable List<Integer> indexIds) {
        Map<Integer, TableSchemaAwareIndexStorage> indexStorageById = indexStorageById();

        if (indexIds == null) {
            return indexStorageById.values();
        }

        var res = new ArrayList<TableSchemaAwareIndexStorage>(indexIds.size());

        for (Integer indexId : indexIds) {
            TableSchemaAwareIndexStorage index = indexStorageById.get(indexId);

            // We assume that if the index is missing in indexStorageById, then it has begun to be destroyed and we do not need it.
            if (index != null) {
                res.add(index);
            }
        }

        return res;
    }

    private TableSchemaAwareIndexStorage[] indexesSnapshot(@Nullable List<Integer> indexIds) {
        Map<Integer, TableSchemaAwareIndexStorage> indexStorageById = indexStorageById();

        if (indexIds == null) {
            return indexStorageById.values().toArray(TableSchemaAwareIndexStorage[]::new);
        }

        var res = new ArrayList<TableSchemaAwareIndexStorage>(indexIds.size());

        for (Integer indexId : indexIds) {
            TableSchemaAwareIndexStorage index = indexStorageById.get(indexId);

            // We assume that if the index is missing in indexStorageById, then it has begun to be destroyed and we do not need it.
            if (index != null) {
                res.add(index);
            }
        }

        return res.toArray(TableSchemaAwareIndexStorage[]::new);
    }

    /**
     * Returns the row ID for which the index needs to be built, {@code null} means that the index building has completed.
     *
     * @param indexId Index ID of interest.
     * @throws StorageException If failed to get the row ID.
     */
    public @Nullable RowId getNextRowIdToBuildIndex(int indexId) {
        TableSchemaAwareIndexStorage indexStorage = indexStorageById().get(indexId);

        // We assume that if the index is missing in indexStorageById, then it has begun to be destroyed and we do not need it.
        if (indexStorage == null) {
            return null;
        }

        return getNextRowIdToBuildFromIndex(indexStorage);
    }

    /**
     * Sets the row ID for which the index needs to be built, {@code null} means that the index is built.
     *
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.</p>
     *
     * @param indexId Index ID of interest.
     * @param rowId Row ID.
     * @throws StorageException If failed to set the row ID.
     */
    public void setNextRowIdToBuildIndex(int indexId, @Nullable RowId rowId) {
        TableSchemaAwareIndexStorage indexStorage = indexStorageById().get(indexId);

        // We assume that if the index is missing in indexStorageById, then it has begun to be destroyed and we do not need it.
        if (indexStorage != null) {
            setNextRowIdToBuildToIndex(indexStorage, rowId);
        }
    }

    private Map<Integer, TableSchemaAwareIndexStorage> indexStorageById() {
        Map<Integer, TableSchemaAwareIndexStorage> indexStorageById = indexes.get();

        assert !indexStorageById.isEmpty();

        return indexStorageById;
    }

    private static void putToIndex(TableSchemaAwareIndexStorage indexStorage, BinaryRow binaryRow, RowId rowId) {
        try {
            indexStorage.put(binaryRow, rowId);
        } catch (StorageDestroyedException ignore) {
            // Index is in the process of being destroyed, which means there is no need to write to it.
        }
    }

    private static void removeFromIndex(TableSchemaAwareIndexStorage indexStorage, BinaryRow binaryRow, RowId rowId) {
        try {
            indexStorage.remove(binaryRow, rowId);
        } catch (StorageDestroyedException ignore) {
            // Index is in the process of being destroyed, which means there is no need to write to it.
        }
    }

    private static void setNextRowIdToBuildToIndex(TableSchemaAwareIndexStorage indexStorage, @Nullable RowId nextRowIdToBuild) {
        try {
            indexStorage.storage().setNextRowIdToBuild(nextRowIdToBuild);
        } catch (StorageDestroyedException ignore) {
            // Index is in the process of being destroyed, which means there is no need to write to it.
        }
    }

    private static @Nullable RowId getNextRowIdToBuildFromIndex(TableSchemaAwareIndexStorage indexStorage) {
        try {
            return indexStorage.storage().getNextRowIdToBuild();
        } catch (StorageDestroyedException ignore) {
            // Index is in the process of being destroyed, which means there is no need to write to it.
            return null;
        }
    }
}
