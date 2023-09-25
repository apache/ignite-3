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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
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
import org.apache.ignite.internal.table.distributed.replicator.PendingRows;
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

    /** A container for rows that were inserted, updated or removed. */
    private final PendingRows pendingRows = new PendingRows();

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
     * @param trackWriteIntent If {@code true} then write intent should be tracked.
     * @param onApplication Callback on application.
     * @param commitTs Commit timestamp to use on autocommit.
     */
    public void handleUpdate(
            UUID txId,
            UUID rowUuid,
            TablePartitionId commitPartitionId,
            @Nullable BinaryRow row,
            boolean trackWriteIntent,
            @Nullable Runnable onApplication,
            @Nullable HybridTimestamp commitTs
    ) {
        indexUpdateHandler.waitIndexes();

        storage.runConsistently(locker -> {
            RowId rowId = new RowId(partitionId, rowUuid);
            int commitTblId = commitPartitionId.tableId();
            int commitPartId = commitPartitionId.partitionId();

            locker.lock(rowId);

            if (commitTs != null) {
                storage.addWriteCommitted(rowId, row, commitTs);
            } else {
                BinaryRow oldRow = storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

                if (oldRow != null) {
                    assert commitTs == null : String.format("Expecting explicit txn: [txId=%s]", txId);
                    // Previous uncommitted row should be removed from indexes.
                    tryRemovePreviousWritesIndex(rowId, oldRow);
                }
            }

            indexUpdateHandler.addToIndexes(row, rowId);

            if (trackWriteIntent) {
                pendingRows.addPendingRowId(txId, rowId);
            }

            if (onApplication != null) {
                onApplication.run();
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
     * @param trackWriteIntent If {@code true} then write intent should be tracked.
     * @param onApplication Callback on application.
     * @param commitTs Commit timestamp to use on autocommit.
     */
    public void handleUpdateAll(
            UUID txId,
            Map<UUID, BinaryRowMessage> rowsToUpdate,
            TablePartitionId commitPartitionId,
            boolean trackWriteIntent,
            @Nullable Runnable onApplication,
            @Nullable HybridTimestamp commitTs
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

                    if (commitTs != null) {
                        storage.addWriteCommitted(rowId, row, commitTs);
                    } else {
                        BinaryRow oldRow = storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

                        if (oldRow != null) {
                            assert commitTs == null : String.format("Expecting explicit txn: [txId=%s]", txId);
                            // Previous uncommitted row should be removed from indexes.
                            tryRemovePreviousWritesIndex(rowId, oldRow);
                        }
                    }

                    rowIds.add(rowId);
                    indexUpdateHandler.addToIndexes(row, rowId);
                }

                if (trackWriteIntent) {
                    pendingRows.addPendingRowIds(txId, rowIds);
                }

                if (onApplication != null) {
                    onApplication.run();
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
     * Handles the read of a write-intent.
     *
     * @param txId Transaction id.
     * @param rowId Row id.
     */
    public void handleWriteIntentRead(UUID txId, RowId rowId) {
        pendingRows.addPendingRowId(txId, rowId);
    }

    /**
     * Handles the cleanup of a transaction. The transaction is either committed or rolled back.
     *
     * @param txId Transaction id.
     * @param commit Commit flag. {@code true} if transaction is committed, {@code false} otherwise.
     * @param commitTimestamp Commit timestamp. Not {@code null} if {@code commit} is {@code true}.
     */
    public void handleTransactionCleanup(UUID txId, boolean commit, @Nullable HybridTimestamp commitTimestamp) {
        handleTransactionCleanup(txId, commit, commitTimestamp, null);
    }

    /**
     * Handles the cleanup of a transaction. The transaction is either committed or rolled back.
     *
     * @param txId Transaction id.
     * @param commit Commit flag. {@code true} if transaction is committed, {@code false} otherwise.
     * @param commitTimestamp Commit timestamp. Not {@code null} if {@code commit} is {@code true}.
     * @param onApplication On application callback.
     */
    public void handleTransactionCleanup(
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable Runnable onApplication) {
        Set<RowId> pendingRowIds = pendingRows.removePendingRowIds(txId);

        // `pendingRowIds` might be empty when we have already cleaned up the storage for this transaction,
        // for example, when primary (PartitionReplicaListener) is collocated with the raft node (PartitionListener)
        // and one of them has already processed the cleanup request, since they share the instance of this class.
        // However, we still need to run `onApplication` if it is not null, e.g. called in TxCleanupCommand handler in PartitionListener
        // to update indexes. In this case it should be executed under `runConsistently`.
        if (!pendingRowIds.isEmpty() || onApplication != null) {
            storage.runConsistently(locker -> {
                pendingRowIds.forEach(locker::lock);

                if (commit) {
                    performCommitWrite(pendingRowIds, commitTimestamp);
                } else {
                    performAbortWrite(pendingRowIds);
                }

                if (onApplication != null) {
                    onApplication.run();
                }

                return null;
            });

        }
    }

    /**
     * Commit write intents created by the provided transaction.
     *
     * @param pendingRowIds Row ids of write-intents to be committed.
     * @param commitTimestamp Commit timestamp.
     */
    private void performCommitWrite(Set<RowId> pendingRowIds, HybridTimestamp commitTimestamp) {
        pendingRowIds.forEach(rowId -> storage.commitWrite(rowId, commitTimestamp));
    }

    /**
     * Abort write intents created by the provided transaction.
     *
     * @param pendingRowIds Row ids of write-intents to be aborted.
     */
    private void performAbortWrite(Set<RowId> pendingRowIds) {
        for (RowId rowId : pendingRowIds) {
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
    }

    /**
     * Returns partition index update handler.
     */
    public IndexUpdateHandler getIndexUpdateHandler() {
        return indexUpdateHandler;
    }
}
