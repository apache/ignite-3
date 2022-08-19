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

import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * The command updates a row specified by the row id specified.
 */
public class UpdateCommand extends PartitionCommand implements WriteCommand {
    /** Id of row that will be updated. */
    private final RowId rowId;

    /** Binary row. */
    private BinaryRow row;

    /**
     * Creates a new instance of UpdateCommand with the given row to be updated. The {@code row} or {@code rowId} should not be {@code
     * null}.
     *
     * @param rowId Row id.
     * @param row   Binary row.
     * @param txId  The transaction id.
     * @see PartitionCommand
     */
    public UpdateCommand(@NotNull RowId rowId, @NotNull BinaryRow row, @NotNull UUID txId) {
        super(txId);

        this.rowId = rowId;
        this.row = row;
    }

    /**
     * Gets a row id that will update.
     *
     * @return Row id.
     */
    public RowId getRowId() {
        return rowId;
    }

    /**
     * Gets a binary row.
     *
     * @return Binary key.
     */
    public BinaryRow getRow() {
        return row;
    }
}
