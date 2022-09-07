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
import java.util.UUID;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * The command deletes entries by the passed row ids.
 */
public class DeleteAllCommand extends PartitionCommand implements WriteCommand {
    /** Ids of the rows that will be deleted. */
    private final Collection<RowId> rowIds;

    /**
     * Creates a new instance of DeleteAllCommand with the given set of keys to be deleted. The {@code keyRows} should not be {@code null}
     * or empty.
     *
     * @param rowIds Collection of row ids to be deleted.
     * @param txId   Transaction id.
     * @see PartitionCommand
     */
    public DeleteAllCommand(@NotNull Collection<RowId> rowIds, @NotNull UUID txId) {
        super(txId);
        this.rowIds = rowIds;
    }

    /**
     * Gets rows that will be deleted.
     *
     * @return Row ids.
     */
    public Collection<RowId> getRowIds() {
        return rowIds;
    }
}
