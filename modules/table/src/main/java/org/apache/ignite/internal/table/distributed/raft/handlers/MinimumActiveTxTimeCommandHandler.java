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

package org.apache.ignite.internal.table.distributed.raft.handlers;

import static org.apache.ignite.internal.partition.replicator.raft.CommandResult.EMPTY_APPLIED_RESULT;
import static org.apache.ignite.internal.partition.replicator.raft.CommandResult.EMPTY_NOT_APPLIED_RESULT;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateMinimumActiveTxBeginTimeCommand;
import org.apache.ignite.internal.partition.replicator.raft.CommandResult;
import org.apache.ignite.internal.partition.replicator.raft.handlers.AbstractCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.jetbrains.annotations.Nullable;

/**
 * RAFT command handler that process {@link UpdateMinimumActiveTxBeginTimeCommand} commands.
 */
public class MinimumActiveTxTimeCommandHandler extends AbstractCommandHandler<UpdateMinimumActiveTxBeginTimeCommand> {
    /** Data storage to which the command will be applied. */
    private final PartitionDataStorage storage;

    /**
     * Table partition identifier.
     * {@link TablePartitionId} is used here instead of {@link org.apache.ignite.internal.replicator.ZonePartitionId}
     * intentionally because we are not going to re-work catalog compaction internals {@link MinimumRequiredTimeCollectorService}.
     **/
    private final TablePartitionId tablePartitionId;

    /** Service that collects minimum required timestamp for each partition. */
    private final MinimumRequiredTimeCollectorService minTimeCollectorService;

    /**
     * Creates a new instance of the command handler.
     *
     * @param storage Partition data storage.
     * @param tablePartitionId Table partition identifier.
     * @param minTimeCollectorService Minimum required time collector service.
     */
    public MinimumActiveTxTimeCommandHandler(
            PartitionDataStorage storage,
            TablePartitionId tablePartitionId,
            MinimumRequiredTimeCollectorService minTimeCollectorService
    ) {
        this.storage = storage;
        this.tablePartitionId = tablePartitionId;
        this.minTimeCollectorService = minTimeCollectorService;
    }

    @Override
    protected CommandResult handleInternally(
            UpdateMinimumActiveTxBeginTimeCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return EMPTY_NOT_APPLIED_RESULT;
        }

        long timestamp = command.timestamp();

        storage.flush(false)
                .whenComplete((r, t) -> {
                    if (t == null) {
                        minTimeCollectorService.recordMinActiveTxTimestamp(tablePartitionId, timestamp);
                    }
                });

        // This command does not update last applied index because it does not change the storage state.

        return EMPTY_APPLIED_RESULT;
    }
}
