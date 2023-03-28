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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
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

    /** Last recorded GC low watermark. */
    private final AtomicReference<HybridTimestamp> lastRecordedLwm = new AtomicReference<>();

    /** Data storage configuration. */
    private final DataStorageConfiguration dsCfg;

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param storage Partition data storage.
     * @param indexes Indexes supplier.
     * @param dsCfg Data storage configuration.
     */
    public StorageUpdateHandler(
            int partitionId,
            PartitionDataStorage storage,
            Supplier<Map<UUID, TableSchemaAwareIndexStorage>> indexes,
            DataStorageConfiguration dsCfg
    ) {
        this.partitionId = partitionId;
        this.storage = storage;
        this.indexes = indexes;
        this.dsCfg = dsCfg;
    }

    /**
     * Handles single update.
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
        handleUpdate(txId, rowUuid, commitPartitionId, rowBuffer, onReplication, null);
    }

    /**
     * Handles single update.
     *
     * @param txId Transaction id.
     * @param rowUuid Row UUID.
     * @param commitPartitionId Commit partition id.
     * @param rowBuffer Row buffer.
     * @param onReplication Callback on replication.
     * @param lowWatermark GC low watermark.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-18909 Pass low watermark.
    public void handleUpdate(
            UUID txId,
            UUID rowUuid,
            TablePartitionId commitPartitionId,
            @Nullable ByteBuffer rowBuffer,
            @Nullable Consumer<RowId> onReplication,
            @Nullable HybridTimestamp lowWatermark
    ) {
        storage.runConsistently(() -> {
            executeBatchGc(lowWatermark);

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
     * Handles multiple updates.
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
        handleUpdateAll(txId, rowsToUpdate, commitPartitionId, onReplication, null);
    }

    /**
     * Handle multiple updates.
     *
     * @param txId Transaction id.
     * @param rowsToUpdate Collection of rows to update.
     * @param commitPartitionId Commit partition id.
     * @param onReplication On replication callback.
     * @param lowWatermark GC low watermark.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-18909 Pass low watermark.
    public void handleUpdateAll(
            UUID txId,
            Map<UUID, ByteBuffer> rowsToUpdate,
            TablePartitionId commitPartitionId,
            @Nullable Consumer<Collection<RowId>> onReplication,
            @Nullable HybridTimestamp lowWatermark
    ) {
        storage.runConsistently(() -> {
            executeBatchGc(lowWatermark);

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

    private void executeBatchGc(@Nullable HybridTimestamp newLwm) {
        if (newLwm == null) {
            return;
        }

        @Nullable HybridTimestamp oldLwm;
        do {
            oldLwm = lastRecordedLwm.get();

            if (oldLwm != null && newLwm.compareTo(oldLwm) <= 0) {
                break;
            }
        } while (!lastRecordedLwm.compareAndSet(oldLwm, newLwm));

        if (oldLwm == null || newLwm.compareTo(oldLwm) > 0) {
            // Iff the lwm we have is the new lwm.
            // Otherwise our newLwm is either smaller than last recorded lwm or last recorded lwm has changed
            // concurrently and it become greater. If that's the case, another thread will perform the GC.
            vacuumBatch(newLwm, dsCfg.gcOnUpdateBatchSize().value());
        }
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

                    // If any of the previous versions' index value equals the index value of
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
     * @return {@code true} if an entry was garbage collected, {@code false} if there was nothing to collect.
     * @see MvPartitionStorage#pollForVacuum(HybridTimestamp)
     */
    public boolean vacuum(HybridTimestamp lowWatermark) {
        return storage.runConsistently(() -> internalVacuum(lowWatermark));
    }

    /**
     * Tries removing {@code count} oldest stale entries and their indexes.
     * If there's less entries that can be removed, then exits prematurely.
     *
     * @param lowWatermark Low watermark for the vacuum.
     * @param count Count of entries to GC.
     */
    private void vacuumBatch(HybridTimestamp lowWatermark, int count) {
        storage.runConsistently(() -> {
            for (int i = 0; i < count; i++) {
                if (!internalVacuum(lowWatermark)) {
                    break;
                }
            }

            return null;
        });
    }

    /**
     * Executes garbage collection. Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.
     *
     * @param lowWatermark Low watermark for the vacuum.
     * @return {@code true} if an entry was garbage collected, {@code false} if there was nothing to collect.
     */
    private boolean internalVacuum(HybridTimestamp lowWatermark) {
        BinaryRowAndRowId vacuumed = storage.pollForVacuum(lowWatermark);

        if (vacuumed == null) {
            // Nothing was garbage collected.
            return false;
        }

        BinaryRow binaryRow = vacuumed.binaryRow();

        assert binaryRow != null;

        RowId rowId = vacuumed.rowId();

        try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
            tryRemoveFromIndexes(binaryRow, rowId, cursor);
        }

        return true;
    }

    /**
     * Adds a binary row to the indexes, if the tombstone then skips such operation.
     */
    public void addToIndexes(@Nullable BinaryRow binaryRow, RowId rowId) {
        if (binaryRow == null) { // skip removes
            return;
        }

        for (TableSchemaAwareIndexStorage index : indexes.get().values()) {
            index.put(binaryRow, rowId);
        }
    }

    /**
     * Waits for indexes to be created.
     */
    // TODO: IGNITE-18619 Fix it, we should have already waited for the indexes to be created
    public void waitIndexes() {
        indexes.get();
    }

    /**
     * Builds an index for all versions of a row.
     *
     * <p>Index is expected to exist, skips the tombstones.
     *
     * @param indexId Index ID.
     * @param rowUuids Row uuids.
     * @param finish Index build completion flag.
     */
    public void buildIndex(UUID indexId, List<UUID> rowUuids, boolean finish) {
        TableSchemaAwareIndexStorage index = indexes.get().get(indexId);

        assert index != null : "indexId=" + indexId + ", partitionId=" + partitionId;

        RowId lastRowId = null;

        for (UUID rowUuid : rowUuids) {
            lastRowId = new RowId(partitionId, rowUuid);

            try (Cursor<ReadResult> cursor = storage.scanVersions(lastRowId)) {
                while (cursor.hasNext()) {
                    ReadResult next = cursor.next();

                    if (!next.isEmpty()) {
                        index.put(next.binaryRow(), lastRowId);
                    }
                }
            }
        }

        assert lastRowId != null || finish : "indexId=" + indexId + ", partitionId=" + partitionId;

        index.storage().setNextRowIdToBuild(finish ? null : lastRowId);
    }
}
