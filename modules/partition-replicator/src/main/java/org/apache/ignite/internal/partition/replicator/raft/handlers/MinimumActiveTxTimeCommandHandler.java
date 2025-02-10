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

package org.apache.ignite.internal.partition.replicator.raft.handlers;

import java.io.Serializable;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateMinimumActiveTxBeginTimeCommand;
import org.apache.ignite.internal.partition.replicator.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.TablePartitionId;

/**
 * RAFT command handler that process {@link UpdateMinimumActiveTxBeginTimeCommand} commands.
 */
public class MinimumActiveTxTimeCommandHandler {
    private final MinimumRequiredTimeCollectorService minTimeCollectorService;

    public MinimumActiveTxTimeCommandHandler(MinimumRequiredTimeCollectorService minTimeCollectorService) {
        this.minTimeCollectorService = minTimeCollectorService;
    }

    /**
     * Handles {@link UpdateMinimumActiveTxBeginTimeCommand} command.
     *
     * @param cmd Command to be processed.
     * @param commandIndex Command index.
     * @param listener Table commands processor.
     * @param partitionId Table partition identifier.
     * @return Pair that represents command processing.
     *     The first parameter is always {@code null}, and the second one is boolean that indicates.
     */
    public IgniteBiTuple<Serializable, Boolean> handle(
            UpdateMinimumActiveTxBeginTimeCommand cmd,
            long commandIndex,
            RaftGroupListener listener,
            TablePartitionId partitionId
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= listener.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        long timestamp = cmd.timestamp();

        listener
                .flushStorage(commandIndex)
                .whenComplete((r, t) -> {
                    if (t == null) {
                        minTimeCollectorService.recordMinActiveTxTimestamp(partitionId, timestamp);
                    }
                });

        return new IgniteBiTuple<>(null, true);
    }
}
