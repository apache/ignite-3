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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Handler for storage updates that can be performed on processing of primary replica requests and partition replication requests.
 */
public class StorageUpdateHandler {
    /** Partition id. */
    private final int partitionId;

    /** Partition storage with access to MV data of a partition. */
    private final PartitionDataStorage storage;

    private final Supplier<Map<UUID, TableSchemaAwareIndexStorage>> indexes;

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param storage Partition data storage.
     * @param indexes Indexes supplier.
     */
    public StorageUpdateHandler(int partitionId, PartitionDataStorage storage, Supplier<Map<UUID, TableSchemaAwareIndexStorage>> indexes) {
        this.partitionId = partitionId;
        this.storage = storage;
        this.indexes = indexes;
    }

    /**
     * Handle single update.
     *
     * @param txId Transaction id.
     * @param rowUuid Row UUID.
     * @param commitPartitionId Commit partition id.
     * @param rowBuffer Row buffer.
     * @param onReplication Callback on replication.
     */
    public void handleUpdate(
            UUID txId,
            UUID rowUuid,
            TablePartitionId commitPartitionId,
            @Nullable ByteBuffer rowBuffer,
            @Nullable Consumer<RowId> onReplication
    ) {
        storage.runConsistently(() -> {
            BinaryRow row = rowBuffer != null ? new ByteBufferRow(rowBuffer) : null;
            RowId rowId = new RowId(partitionId, rowUuid);
            UUID commitTblId = commitPartitionId.tableId();
            int commitPartId = commitPartitionId.partitionId();

            BinaryRow oldRow = storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

            if (oldRow != null) {
                // Previous uncommitted row should be removed from indexes.
                tryRemovePreviousWritesIndex(rowId, oldRow);
            }

            if (onReplication != null) {
                onReplication.accept(rowId);
            }

            addToIndexes(row, rowId);

            return null;
        });
    }

    /**
     * Handle multiple updates.
     *
     * @param txId Transaction id.
     * @param rowsToUpdate Collection of rows to update.
     * @param commitPartitionId Commit partition id.
     * @param onReplication On replication callback.
     */
    public void handleUpdateAll(
            UUID txId,
            Map<UUID, ByteBuffer> rowsToUpdate,
            TablePartitionId commitPartitionId,
            @Nullable Consumer<Collection<RowId>> onReplication
    ) {
        storage.runConsistently(() -> {
            UUID commitTblId = commitPartitionId.tableId();
            int commitPartId = commitPartitionId.partitionId();

            if (!nullOrEmpty(rowsToUpdate)) {
                List<RowId> rowIds = new ArrayList<>();

                for (Map.Entry<UUID, ByteBuffer> entry : rowsToUpdate.entrySet()) {
                    RowId rowId = new RowId(partitionId, entry.getKey());
                    BinaryRow row = entry.getValue() != null ? new ByteBufferRow(entry.getValue()) : null;

                    BinaryRow oldRow = storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

                    if (oldRow != null) {
                        // Previous uncommitted row should be removed from indexes.
                        tryRemovePreviousWritesIndex(rowId, oldRow);
                    }

                    rowIds.add(rowId);
                    addToIndexes(row, rowId);
                }

                if (onReplication != null) {
                    onReplication.accept(rowIds);
                }
            }

            return null;
        });
    }

    /**
     * Tries to remove a previous write from index.
     *
     * @param rowId Row id.
     * @param previousRow Previous write value.
     */
    private void tryRemovePreviousWritesIndex(RowId rowId, BinaryRow previousRow) {
        try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
            if (!cursor.hasNext()) {
                return;
            }

            tryRemoveFromIndexes(previousRow, rowId, cursor);
        }
    }

    /**
     * Handles the abortion of a transaction.
     *
     * @param pendingRowIds Row ids of write-intents to be rolled back.
     * @param onReplication On replication callback.
     */
    public void handleTransactionAbortion(Set<RowId> pendingRowIds, Runnable onReplication) {
        storage.runConsistently(() -> {
            for (RowId rowId : pendingRowIds) {
                try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                    if (!cursor.hasNext()) {
                        continue;
                    }

                    ReadResult item = cursor.next();

                    assert item.isWriteIntent();

                    BinaryRow rowToRemove = item.binaryRow();

                    if (rowToRemove == null) {
                        continue;
                    }

                    tryRemoveFromIndexes(rowToRemove, rowId, cursor);
                }
            }

            pendingRowIds.forEach(storage::abortWrite);

            onReplication.run();

            return null;
        });
    }

    /**
     * Tries removing indexed row from every index.
     * Removes the row only if no previous value's index matches index of the row to remove, because if it matches, then the index
     * might still be in use.
     *
     * @param rowToRemove Row to remove from indexes.
     * @param rowId Row id.
     * @param previousValues Cursor with previous version of the row.
     */
    private void tryRemoveFromIndexes(BinaryRow rowToRemove, RowId rowId, Cursor<ReadResult> previousValues) {
        TableSchemaAwareIndexStorage[] indexes = this.indexes.get().values().toArray(new TableSchemaAwareIndexStorage[0]);

        ByteBuffer[] indexValues = new ByteBuffer[indexes.length];

        // Precalculate value for every index.
        for (int i = 0; i < indexes.length; i++) {
            TableSchemaAwareIndexStorage index = indexes[i];

            indexValues[i] = index.resolveIndexRow(rowToRemove).byteBuffer();
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

                    // If any of the previous versions' index value matches the index value of
                    // the row to remove, then we can't remove that index as it can still be used.
                    BinaryTuple previousRowIndex = index.resolveIndexRow(previousRow);

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
     * Tries removing partition's oldest stale entry and its indexes.
     *
     * @param lowWatermark Low watermark for the vacuum.
     */
    public boolean vacuum(HybridTimestamp lowWatermark) {
        return storage.runConsistently(() -> {
            BinaryRowAndRowId vacuumed = storage.pollForVacuum(lowWatermark);

            if (vacuumed == null) {
                // Nothing was garbage collected.
                return false;
            }

            BinaryRow binaryRow = vacuumed.binaryRow();

            assert binaryRow != null;

            RowId rowId = vacuumed.rowId();

            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                assert cursor.hasNext();

                tryRemoveFromIndexes(binaryRow, rowId, cursor);
            }

            return true;
        });
    }

    private void addToIndexes(@Nullable BinaryRow binaryRow, RowId rowId) {
        if (binaryRow == null) { // skip removes
            return;
        }

        for (TableSchemaAwareIndexStorage index : indexes.get().values()) {
            index.put(binaryRow, rowId);
        }
    }
}
