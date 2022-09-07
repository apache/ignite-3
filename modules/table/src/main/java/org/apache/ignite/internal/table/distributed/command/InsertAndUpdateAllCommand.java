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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * The command inserts or update a batch rows.
 */
public class InsertAndUpdateAllCommand extends PartitionCommand implements WriteCommand {
    /** Binary rows. */
    private transient Collection<BinaryRow> rows;

    /** Rows to update. */
    private final Map<RowId, BinaryRow> rowsToUpdate;

    /**
     * Creates a new instance of {@link InsertAndUpdateAllCommand} with the given rows to be inserted or updated. The {@code rows} and
     * {@code rowsToUpdate} should not be {@code null} both.
     *
     * @param rows         Binary rows.
     * @param rowsToUpdate Mapping a row id to the row to update.
     * @param txId         Transaction id.
     * @see PartitionCommand
     */
    public InsertAndUpdateAllCommand(Collection<BinaryRow> rows, Map<RowId, BinaryRow> rowsToUpdate, @NotNull UUID txId) {
        super(txId);
        this.rows = rows;
        this.rowsToUpdate = rowsToUpdate;
    }

    /**
     * Gets rows to insert.
     *
     * @return Binary rows.
     */
    public Collection<BinaryRow> getRows() {
        return rows;
    }

    /**
     * Gets rows to update.
     *
     * @return Binary rows.
     */
    public Map<RowId, BinaryRow> getRowsToUpdate() {
        return rowsToUpdate;
    }
}
