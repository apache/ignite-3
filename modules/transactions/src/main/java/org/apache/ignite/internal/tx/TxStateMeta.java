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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateMetaMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction state meta.
 */
public class TxStateMeta implements TransactionMeta {
    private static final long serialVersionUID = 8521181896862227127L;

    private final TxState txState;

    private final @Nullable UUID txCoordinatorId;

    /** ID of the replication group that manages a transaction state. */
    private final @Nullable TablePartitionId commitPartitionId;

    private final @Nullable HybridTimestamp commitTimestamp;

    private final @Nullable Long initialVacuumObservationTimestamp;

    private final @Nullable Long cleanupCompletionTimestamp;

    /**
     * The ignite transaction object is associated with this state. This field can be initialized only on the transaction coordinator,
     * {@code null} in other nodes.
     */
    @IgniteToStringExclude
    private final @Nullable InternalTransaction tx;

    /**
     * Constructor.
     *
     * @param txState Transaction state.
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition replication group id.
     * @param commitTimestamp Commit timestamp.
     * @param tx Transaction object. This parameter is not {@code null} only for transaction coordinator.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable UUID txCoordinatorId,
            @Nullable TablePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable InternalTransaction tx
    ) {
        this(txState, txCoordinatorId, commitPartitionId, commitTimestamp, tx, null);
    }

    /**
     * Constructor.
     *
     * @param txState Transaction state.
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition replication group id.
     * @param commitTimestamp Commit timestamp.
     * @param tx Transaction object. This parameter is not {@code null} only for transaction coordinator.
     * @param initialVacuumObservationTimestamp Initial vacuum observation timestamp.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable UUID txCoordinatorId,
            @Nullable TablePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable InternalTransaction tx,
            @Nullable Long initialVacuumObservationTimestamp
    ) {
        this(txState, txCoordinatorId, commitPartitionId, commitTimestamp, tx, initialVacuumObservationTimestamp, null);
    }

    /**
     * Constructor.
     *
     * @param txState Transaction state.
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition replication group id.
     * @param commitTimestamp Commit timestamp.
     * @param tx Transaction object. This parameter is not {@code null} only for transaction coordinator.
     * @param initialVacuumObservationTimestamp Initial vacuum observation timestamp.
     * @param cleanupCompletionTimestamp Cleanup completion timestamp.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable UUID txCoordinatorId,
            @Nullable TablePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable InternalTransaction tx,
            @Nullable Long initialVacuumObservationTimestamp,
            @Nullable Long cleanupCompletionTimestamp
    ) {
        this.txState = txState;
        this.txCoordinatorId = txCoordinatorId;
        this.commitPartitionId = commitPartitionId;
        this.commitTimestamp = commitTimestamp;
        this.tx = tx;
        this.cleanupCompletionTimestamp = cleanupCompletionTimestamp;

        if (initialVacuumObservationTimestamp != null) {
            this.initialVacuumObservationTimestamp = initialVacuumObservationTimestamp;
        } else {
            this.initialVacuumObservationTimestamp = TxState.isFinalState(txState) ? coarseCurrentTimeMillis() : null;
        }
    }

    /**
     * Gets a transaction object or {@code null} it the current node is not a coordinator for this transaction.
     *
     * @return Transaction object.
     */
    public @Nullable InternalTransaction tx() {
        return tx;
    }

    /**
     * Creates a transaction state for the same transaction, but this one is marked abandoned.
     *
     * @return Transaction state meta.
     */
    public TxStateMetaAbandoned abandoned() {
        assert checkTransitionCorrectness(txState, ABANDONED) : "Transaction state is incorrect [txState=" + txState + "].";

        return new TxStateMetaAbandoned(txCoordinatorId, commitPartitionId);
    }

    /**
     * Creates a transaction state for the same transaction, but this one is marked finishing.
     *
     * @return Transaction state meta.
     */
    public TxStateMetaFinishing finishing() {
        return new TxStateMetaFinishing(txCoordinatorId, commitPartitionId);
    }

    @Override
    public TxState txState() {
        return txState;
    }

    public @Nullable UUID txCoordinatorId() {
        return txCoordinatorId;
    }

    public @Nullable TablePartitionId commitPartitionId() {
        return commitPartitionId;
    }

    @Override
    public @Nullable HybridTimestamp commitTimestamp() {
        return commitTimestamp;
    }

    public @Nullable Long initialVacuumObservationTimestamp() {
        return initialVacuumObservationTimestamp;
    }

    public @Nullable Long cleanupCompletionTimestamp() {
        return cleanupCompletionTimestamp;
    }

    @Override
    public TxStateMetaMessage toTransactionMetaMessage(
            ReplicaMessagesFactory replicaMessagesFactory,
            TxMessagesFactory txMessagesFactory
    ) {
        return txMessagesFactory.txStateMetaMessage()
                .txState(txState)
                .txCoordinatorId(txCoordinatorId)
                .commitPartitionId(commitPartitionId == null ? null : toTablePartitionIdMessage(replicaMessagesFactory, commitPartitionId))
                .commitTimestamp(commitTimestamp)
                .initialVacuumObservationTimestamp(initialVacuumObservationTimestamp)
                .cleanupCompletionTimestamp(cleanupCompletionTimestamp)
                .build();
    }

    @Override
    public String toString() {
        return S.toString(TxStateMeta.class, this);
    }
}
