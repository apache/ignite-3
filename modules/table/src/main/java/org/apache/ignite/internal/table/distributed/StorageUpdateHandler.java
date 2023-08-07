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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryRowMessage;
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

    /** Garbage collector configuration. */
    private final GcConfiguration gcConfig;

    /** Low watermark. */
    private final LowWatermark lowWatermark;

    /** Partition index update handler. */
    private final IndexUpdateHandler indexUpdateHandler;

    /** Partition gc update handler. */
    private final GcUpdateHandler gcUpdateHandler;

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param storage Partition data storage.
     * @param gcConfig Garbage collector configuration.
     * @param indexUpdateHandler Partition index update handler.
     * @param gcUpdateHandler Partition gc update handler.
     */
    public StorageUpdateHandler(
            int partitionId,
            PartitionDataStorage storage,
            GcConfiguration gcConfig,
            LowWatermark lowWatermark,
            IndexUpdateHandler indexUpdateHandler,
            GcUpdateHandler gcUpdateHandler
    ) {
        this.partitionId = partitionId;
        this.storage = storage;
        this.gcConfig = gcConfig;
        this.lowWatermark = lowWatermark;
        this.indexUpdateHandler = indexUpdateHandler;
        this.gcUpdateHandler = gcUpdateHandler;
    }

    /**
     * Returns partition ID of the storage.
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * Handles single update.
     *
     * @param txId Transaction id.
     * @param rowUuid Row UUID.
     * @param commitPartitionId Commit partition id.
     * @param row Row.
     * @param onApplication Callback on application.
     */
    public void handleUpdate(
            UUID txId,
            UUID rowUuid,
            TablePartitionId commitPartitionId,
            @Nullable BinaryRow row,
            @Nullable Consumer<RowId> onApplication
    ) {
        indexUpdateHandler.waitIndexes();

        storage.runConsistently(locker -> {
            RowId rowId = new RowId(partitionId, rowUuid);
            int commitTblId = commitPartitionId.tableId();
            int commitPartId = commitPartitionId.partitionId();

            locker.lock(rowId);

            BinaryRow oldRow = storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

            if (oldRow != null) {
                // Previous uncommitted row should be removed from indexes.
                tryRemovePreviousWritesIndex(rowId, oldRow);
            }

            indexUpdateHandler.addToIndexes(row, rowId);

            if (onApplication != null) {
                onApplication.accept(rowId);
            }

            return null;
        });

        executeBatchGc();
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
            Map<UUID, BinaryRowMessage> rowsToUpdate,
            TablePartitionId commitPartitionId,
            @Nullable Consumer<Collection<RowId>> onReplication
    ) {
        indexUpdateHandler.waitIndexes();

        storage.runConsistently(locker -> {
            int commitTblId = commitPartitionId.tableId();
            int commitPartId = commitPartitionId.partitionId();

            if (!nullOrEmpty(rowsToUpdate)) {
                List<RowId> rowIds = new ArrayList<>();

                // Sort IDs to prevent deadlock. Natural UUID order matches RowId order within the same partition.
                SortedMap<UUID, BinaryRowMessage> sortedRowsToUpdateMap = new TreeMap<>(rowsToUpdate);

                for (Map.Entry<UUID, BinaryRowMessage> entry : sortedRowsToUpdateMap.entrySet()) {
                    RowId rowId = new RowId(partitionId, entry.getKey());
                    BinaryRow row = entry.getValue() == null ? null : entry.getValue().asBinaryRow();

                    locker.lock(rowId);

                    BinaryRow oldRow = storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

                    if (oldRow != null) {
                        // Previous uncommitted row should be removed from indexes.
                        tryRemovePreviousWritesIndex(rowId, oldRow);
                    }

                    rowIds.add(rowId);
                    indexUpdateHandler.addToIndexes(row, rowId);
                }

                if (onReplication != null) {
                    onReplication.accept(rowIds);
                }
            }

            return null;
        });

        executeBatchGc();
    }

    void executeBatchGc() {
        HybridTimestamp lwm = lowWatermark.getLowWatermark();

        if (lwm == null || gcUpdateHandler.getSafeTimeTracker().current().compareTo(lwm) < 0) {
            return;
        }

        gcUpdateHandler.vacuumBatch(lwm, gcConfig.onUpdateBatchSize().value(), false);
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

            indexUpdateHandler.tryRemoveFromIndexes(previousRow, rowId, cursor);
        }
    }

    /**
     * Handles the abortion of a transaction.
     *
     * @param pendingRowIds Row ids of write-intents to be rolled back.
     * @param onApplication On application callback.
     */
    public void handleTransactionAbortion(Set<RowId> pendingRowIds, Runnable onApplication) {
        storage.runConsistently(locker -> {
            for (RowId rowId : pendingRowIds) {
                locker.lock(rowId);

                try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                    if (!cursor.hasNext()) {
                        continue;
                    }

                    ReadResult item = cursor.next();

                    // TODO: https://issues.apache.org/jira/browse/IGNITE-20124 Prevent double storage updates within primary
                    if (item.isWriteIntent()) {
                        BinaryRow rowToRemove = item.binaryRow();

                        if (rowToRemove == null) {
                            continue;
                        }

                        indexUpdateHandler.tryRemoveFromIndexes(rowToRemove, rowId, cursor);
                    }
                }
            }

            pendingRowIds.forEach(storage::abortWrite);

            onApplication.run();

            return null;
        });
    }

    /**
     * Returns partition index update handler.
     */
    public IndexUpdateHandler getIndexUpdateHandler() {
        return indexUpdateHandler;
    }
}
