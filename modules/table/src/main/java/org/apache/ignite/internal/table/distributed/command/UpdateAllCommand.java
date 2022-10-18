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

package org.apache.ignite.internal.table.distributed.command;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.replicator.message.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.NotNull;

/**
 * State machine command for updating a batch of entries.
 */
public class UpdateAllCommand extends PartitionCommand {
    /** Committed table partition id. */
    private final TablePartitionId commitTablePartitionId;

    /** Rows to update. */
    private transient HashMap<RowId, BinaryRow> rowsToUpdate;

    /** Bytes representation of a row to update map. */
    private byte[] rowsToUpdateBytes;

    /**
     * Constructor for batch remove.
     *
     * @param commitTablePartitionId Committed table partition id.
     * @param removeRows Ids to remove.
     * @param txId Transaction id.
     */
    public UpdateAllCommand(@NotNull TablePartitionId commitTablePartitionId, Collection<RowId> removeRows, @NotNull UUID txId) {
        this(commitTablePartitionId, removeRows, null, txId);
    }

    /**
     * Constructor for a batch update.
     *
     * @param commitTablePartitionId Committed table partition id.
     * @param rowsToUpdate Rows to update or insert.
     * @param txId Transaction id.
     */
    public UpdateAllCommand(@NotNull TablePartitionId commitTablePartitionId, Map<RowId, BinaryRow> rowsToUpdate, @NotNull UUID txId) {
        this(commitTablePartitionId, null, rowsToUpdate, txId);
    }

    /**
     * The constructor.
     *
     * @param commitTablePartitionId Committed table partition id.
     * @param removeRows Ids to remove.
     * @param rowsToUpdate Rows to update or insert.
     * @param txId Transaction id.
     */
    private UpdateAllCommand(
            @NotNull TablePartitionId commitTablePartitionId,
            Collection<RowId> removeRows,
            Map<RowId, BinaryRow> rowsToUpdate,
            @NotNull UUID txId
    ) {
        super(txId);

        this.commitTablePartitionId = commitTablePartitionId;

        int size = (removeRows == null ? 0 : removeRows.size()) + (rowsToUpdate == null ? 0 : rowsToUpdate.size());

        HashMap<RowId, BinaryRow> rows = new HashMap<>(size);

        if (!CollectionUtils.nullOrEmpty(removeRows)) {
            removeRows.forEach(rowId -> rows.put(rowId, null));
        }

        if (!CollectionUtils.nullOrEmpty(rowsToUpdate)) {
            rows.putAll(rowsToUpdate);
        }

        this.rowsToUpdate = rows;

        rowsToUpdateBytes = CommandUtils.rowMapToBytes(rows);
    }

    /**
     * Gets a table partition id that the commit partition.
     *
     * @return Table partition id.
     */
    public TablePartitionId getCommitTablePartitionId() {
        return commitTablePartitionId;
    }


    /**
     * Gets rows to update.
     *
     * @return Rows to update map.
     */
    public HashMap<RowId, BinaryRow> getRowsToUpdate() {
        if (rowsToUpdate == null) {
            rowsToUpdate = new HashMap();

            CommandUtils.readRowMap(rowsToUpdateBytes, rowsToUpdate::put);
        }

        return rowsToUpdate;
    }
}
