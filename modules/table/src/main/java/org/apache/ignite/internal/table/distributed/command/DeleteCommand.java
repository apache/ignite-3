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
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * The command deletes a entry by passed row id.
 */
public class DeleteCommand extends PartitionCommand implements WriteCommand {
    /** Id of the row to be deleted. */
    private final RowId rowId;

    /**
     * Creates a new instance of DeleteCommand with the given row id to be deleted. The {@code rowId} should not be {@code null}.
     *
     * @param txId  Transaction id.
     * @param rowId Row id.
     * @see PartitionCommand
     */
    public DeleteCommand(@NotNull RowId rowId, @NotNull UUID txId) {
        super(txId);
        this.rowId = rowId;
    }

    /**
     * Gets a row id to be deleted.
     *
     * @return Row id.
     */
    public RowId getRowId() {
        return rowId;
    }
}
