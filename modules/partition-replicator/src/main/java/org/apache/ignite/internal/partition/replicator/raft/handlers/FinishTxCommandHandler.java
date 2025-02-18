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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.raft.RaftTxFinishMarker;
import org.apache.ignite.internal.partition.replicator.raft.UnexpectedTransactionStateException;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.tx.message.EnlistedPartitionGroupMessage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;

/**
 * Handler for {@link FinishTxCommand}.
 */
public class FinishTxCommandHandler {
    private static final IgniteLogger LOG = Loggers.forClass(FinishTxCommandHandler.class);

    private final TxStatePartitionStorage txStatePartitionStorage;
    private final TablePartitionId replicationGroupId;

    private final RaftTxFinishMarker txFinishMarker;

    /** Constructor. */
    public FinishTxCommandHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            TablePartitionId replicationGroupId,
            TxManager txManager
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.replicationGroupId = replicationGroupId;

        txFinishMarker = new RaftTxFinishMarker(txManager);
    }

    /**
     * Handles a {@link FinishTxCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @return The actually stored transaction state {@link TransactionResult}.
     * @throws IgniteInternalException if an exception occurred during a transaction state change.
     */
    public IgniteBiTuple<Serializable, Boolean> handle(FinishTxCommand cmd, long commandIndex, long commandTerm)
            throws IgniteInternalException {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= txStatePartitionStorage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        UUID txId = cmd.txId();

        TxState stateToSet = cmd.commit() ? COMMITTED : ABORTED;

        TxMeta txMetaToSet = new TxMeta(
                stateToSet,
                fromPartitionMessages(cmd.partitions()),
                cmd.commitTimestamp()
        );

        TxMeta txMetaBeforeCas = txStatePartitionStorage.get(txId);

        boolean txStateChangeRes = txStatePartitionStorage.compareAndSet(
                txId,
                null,
                txMetaToSet,
                commandIndex,
                commandTerm
        );

        // Assume that we handle the finish command only on the commit partition.
        txFinishMarker.markFinished(txId, cmd.commit(), cmd.commitTimestamp(), this.replicationGroupId);

        LOG.debug("Finish the transaction txId = {}, state = {}, txStateChangeRes = {}", txId, txMetaToSet, txStateChangeRes);

        if (!txStateChangeRes) {
            assert txMetaBeforeCas != null : "txMetaBeforeCase is null, but CAS has failed for " + txId;

            onTxStateStorageCasFail(txId, txMetaBeforeCas, txMetaToSet);
        }

        return new IgniteBiTuple<>(new TransactionResult(stateToSet, cmd.commitTimestamp()), true);
    }

    private static List<EnlistedPartitionGroup> fromPartitionMessages(List<EnlistedPartitionGroupMessage> messages) {
        List<EnlistedPartitionGroup> list = new ArrayList<>(messages.size());

        for (EnlistedPartitionGroupMessage message : messages) {
            list.add(message.asPartitionInfo());
        }

        return list;
    }

    private static void onTxStateStorageCasFail(UUID txId, TxMeta txMetaBeforeCas, TxMeta txMetaToSet) {
        String errorMsg = format("Failed to update tx state in the storage, transaction txId = {} because of inconsistent state,"
                        + " expected state = {}, state to set = {}",
                txId,
                txMetaBeforeCas,
                txMetaToSet
        );

        IgniteInternalException stateChangeException =
                new UnexpectedTransactionStateException(
                        errorMsg,
                        new TransactionResult(txMetaBeforeCas.txState(), txMetaBeforeCas.commitTimestamp())
                );

        // Exception is explicitly logged because otherwise it can be lost if it did not occur on the leader.
        LOG.error(errorMsg);

        throw stateChangeException;
    }
}
