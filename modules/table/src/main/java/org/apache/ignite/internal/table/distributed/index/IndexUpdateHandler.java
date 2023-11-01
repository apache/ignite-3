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

import static org.apache.ignite.internal.tracing.TracingManager.span;

import java.nio.ByteBuffer;
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
import org.apache.ignite.internal.tracing.TraceSpan;
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
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.
     *
     * @param binaryRow Binary row to insert.
     * @param rowId Row ID.
     */
    public void addToIndexes(@Nullable BinaryRow binaryRow, RowId rowId) {
        if (binaryRow == null) { // skip removes
            return;
        }

        try (TraceSpan ignored = span("IndexUpdateHandler.addToIndexes")) {
            for (TableSchemaAwareIndexStorage index : indexes.get().values()) {
                index.put(binaryRow, rowId);
            }
        }
    }

    /**
     * Tries removing indexed row from every index.
     * Removes the row only if no previous value's index matches index of the row to remove, because if it matches, then the index
     * might still be in use.
     *
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.
     *
     * @param rowToRemove Row to remove from indexes.
     * @param rowId Row id.
     * @param previousValues Cursor with previous version of the row.
     */
    public void tryRemoveFromIndexes(BinaryRow rowToRemove, RowId rowId, Cursor<ReadResult> previousValues) {
        TableSchemaAwareIndexStorage[] indexes = this.indexes.get().values().toArray(new TableSchemaAwareIndexStorage[0]);

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
}
