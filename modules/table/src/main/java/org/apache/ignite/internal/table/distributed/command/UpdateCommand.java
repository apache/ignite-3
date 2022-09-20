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
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.RowId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The command updates a row specified by the row id specified.
 *
 */
public class UpdateCommand extends PartitionCommand {
    /** Id of row that will be updated. */
    private final RowId rowId;

    /** Binary row. */
    private transient BinaryRow row;

    /** Row bytes. */
    private byte[] rowBytes;

    /**
     * Creates a new instance of UpdateCommand with the given row to be updated. The {@code rowId} should not be {@code null}.
     *
     * @param rowId Row id.
     * @param row   Binary row.
     * @param txId  The transaction id.
     * @see PartitionCommand
     */
    public UpdateCommand(@NotNull RowId rowId, @Nullable BinaryRow row, @NotNull UUID txId) {
        super(txId);

        this.rowId = rowId;
        this.row = row;

        this.rowBytes = CommandUtils.rowToBytes(row);
    }

    /**
     * Constructor for remove operation.
     *
     * @param rowId Row id.
     * @param txId Transaction id.
     */
    public UpdateCommand(@NotNull RowId rowId, @NotNull UUID txId) {
        this(rowId, null, txId);
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
        if (row == null && rowBytes != null) {
            row = new ByteBufferRow(rowBytes);
        }

        return row;
    }
}
