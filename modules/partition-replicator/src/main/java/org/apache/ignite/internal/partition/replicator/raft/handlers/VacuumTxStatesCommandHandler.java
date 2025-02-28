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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.tx.message.VacuumTxStatesCommand;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.jetbrains.annotations.Nullable;

/**
 * RAFT command handler that process {@link VacuumTxStatesCommand} commands.
 */
public class VacuumTxStatesCommandHandler extends AbstractCommandHandler<VacuumTxStatesCommand> {
    /** Storage of transaction metadata. */
    private final TxStatePartitionStorage txStatePartitionStorage;

    /**
     * Creates a new instance of the command handler.
     *
     * @param txStatePartitionStorage Transactions state storage.
     */
    public VacuumTxStatesCommandHandler(TxStatePartitionStorage txStatePartitionStorage) {
        this.txStatePartitionStorage = txStatePartitionStorage;
    }

    @Override
    protected IgniteBiTuple<Serializable, Boolean> handleInternally(
            VacuumTxStatesCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= txStatePartitionStorage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        txStatePartitionStorage.removeAll(command.txIds(), commandIndex, commandTerm);

        return new IgniteBiTuple<>(null, true);
    }
}
