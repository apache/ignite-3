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
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
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
            ByteBuffer rowBuffer,
            @Nullable Consumer<RowId> onReplication
    ) {
        storage.runConsistently(() -> {
            BinaryRow row = rowBuffer != null ? new ByteBufferRow(rowBuffer) : null;
            RowId rowId = new RowId(partitionId, rowUuid);
            UUID commitTblId = commitPartitionId.tableId();
            int commitPartId = commitPartitionId.partitionId();

            storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

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

                    storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

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

    private void addToIndexes(@Nullable BinaryRow tableRow, RowId rowId) {
        if (tableRow == null || !tableRow.hasValue()) { // skip removes
            return;
        }

        for (TableSchemaAwareIndexStorage index : indexes.get().values()) {
            index.put(tableRow, rowId);
        }
    }
}
