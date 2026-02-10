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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommandV2;
import org.apache.ignite.internal.partition.replicator.raft.CommandResult;
import org.apache.ignite.internal.partition.replicator.raft.RaftTxFinishMarker;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.tx.message.EnlistedPartitionGroupMessage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Handler for {@link FinishTxCommand}.
 */
public class FinishTxCommandHandler extends AbstractCommandHandler<FinishTxCommand> {
    private static final IgniteLogger LOG = Loggers.forClass(FinishTxCommandHandler.class);

    private final TxStatePartitionStorage txStatePartitionStorage;
    private final ZonePartitionId replicationGroupId;

    private final RaftTxFinishMarker txFinishMarker;

    /** Constructor. */
    public FinishTxCommandHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            ZonePartitionId replicationGroupId,
            TxManager txManager
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.replicationGroupId = replicationGroupId;

        txFinishMarker = new RaftTxFinishMarker(txManager);
    }

    @Override
    protected CommandResult handleInternally(
            FinishTxCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) throws IgniteInternalException {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= txStatePartitionStorage.lastAppliedIndex()) {
            return CommandResult.EMPTY_NOT_APPLIED_RESULT;
        }

        UUID txId = command.txId();

        TxState stateToSet = command.commit() ? COMMITTED : ABORTED;

        TxMeta txMetaToSet = new TxMeta(
                stateToSet,
                enlistedPartitions(command),
                command.commitTimestamp()
        );

        TxMeta txMetaBeforeCas = txStatePartitionStorage.get(txId);

        boolean txStateChangeRes = txStatePartitionStorage.compareAndSet(
                txId,
                null,
                txMetaToSet,
                commandIndex,
                commandTerm
        );

        LOG.debug("Finish the transaction txId = {}, state = {}, txStateChangeRes = {}", txId, txMetaToSet, txStateChangeRes);

        if (txStateChangeRes) {
            // Assume that we handle the finish command only on the commit partition.
            txFinishMarker.markFinished(txId, command.commit(), command.commitTimestamp(), this.replicationGroupId);

            TransactionResult result = new TransactionResult(stateToSet, command.commitTimestamp());

            return new CommandResult(result, true);
        }

        if (txMetaBeforeCas == null) {
            throw new IgniteInternalException(
                    INTERNAL_ERR,
                    "txMetaBeforeCas is null, but CAS has failed for {}",
                    txId
            );
        }

        TransactionResult existingResult = new TransactionResult(txMetaBeforeCas.txState(), txMetaBeforeCas.commitTimestamp());

        logTxStateStorageCasFail(txId, txMetaBeforeCas, txMetaToSet);

        return new CommandResult(existingResult, false);
    }

    private static List<EnlistedPartitionGroup> fromPartitionMessages(List<EnlistedPartitionGroupMessage> messages) {
        List<EnlistedPartitionGroup> list = new ArrayList<>(messages.size());

        for (EnlistedPartitionGroupMessage message : messages) {
            list.add(message.asPartitionInfo());
        }

        return list;
    }

    private static void logTxStateStorageCasFail(UUID txId, TxMeta txMetaBeforeCas, TxMeta txMetaToSet) {
        String errorMsg = format("Finish command skipped, transaction txId = {}, because transaction state is already set,"
                        + " existing state = {}, state to set = {}",
                txId,
                txMetaBeforeCas,
                txMetaToSet
        );
        LOG.info(errorMsg);
    }

    private static List<EnlistedPartitionGroup> enlistedPartitions(FinishTxCommand command) {
        if (command instanceof FinishTxCommandV2) {
            return fromPartitionMessages(((FinishTxCommandV2) command).partitions());
        }

        throw new IllegalArgumentException("Unknown command: " + command);
    }
}
