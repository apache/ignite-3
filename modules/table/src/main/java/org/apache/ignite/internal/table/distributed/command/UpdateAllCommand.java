/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.NotNull;

/**
 * The command for update batch of entries.
 */
public class UpdateAllCommand extends PartitionCommand {
    /** Rows to update. */
    private transient HashMap<RowId, BinaryRow> rowsToUpdate;

    /** Bytes representation of row to update map. */
    private byte[] rowsToUpdateBytes;

    /**
     * Constructor for batch remove.
     *
     * @param removeRows Ids to remove.
     * @param txId Transaction id.
     */
    public UpdateAllCommand(Collection<RowId> removeRows, @NotNull UUID txId) {
        this(removeRows, null, txId);
    }

    /**
     * Constructor for batch update.
     *
     * @param rowsToUpdate Rows to update or insert.
     * @param txId Transaction id.
     */
    public UpdateAllCommand(Map<RowId, BinaryRow> rowsToUpdate, @NotNull UUID txId) {
        this(null, rowsToUpdate, txId);
    }

    /**
     * The constructor.
     *
     * @param removeRows Ids to remove.
     * @param rowsToUpdate Rows to update or insert.
     * @param txId Transaction id.
     */
    private UpdateAllCommand(Collection<RowId> removeRows, Map<RowId, BinaryRow> rowsToUpdate, @NotNull UUID txId) {
        super(txId);

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
     * Gets rows to update.
     *
     * @return Row to update map.
     */
    public HashMap<RowId, BinaryRow> getRowsToUpdate() {
        if (rowsToUpdate == null) {
            rowsToUpdate = new HashMap();

            CommandUtils.readRowMap(rowsToUpdateBytes, rowsToUpdate::put);
        }

        return rowsToUpdate;
    }
}
