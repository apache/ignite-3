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

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toReplicationGroupIdMessage;
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
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
    private final @Nullable ReplicationGroupId commitPartitionId;

    private final @Nullable HybridTimestamp commitTimestamp;

    private final @Nullable Long initialVacuumObservationTimestamp;

    private final @Nullable Long cleanupCompletionTimestamp;

    private final @Nullable Boolean isFinishedDueToTimeout;

    private final @Nullable String txLabel;

    /**
     * The ignite transaction object is associated with this state.
     * This field can be initialized only on the transaction coordinator, {@code null} on the other nodes.
     * Moreover, this field can be set to {@code null} even on the transaction coordinator under certain circumstances.
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
     * @param isFinishedDueToTimeout {@code true} if the transaction is finished due to timeout.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable UUID txCoordinatorId,
            // TODO Ignite-22522 Use partition instead.
            @Nullable ReplicationGroupId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable InternalTransaction tx,
            @Nullable Boolean isFinishedDueToTimeout
    ) {
        this(txState, txCoordinatorId, commitPartitionId, commitTimestamp, tx, null, null, isFinishedDueToTimeout, null);
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
     * @param isFinishedDueToTimeout {@code true} if the transaction is finished due to timeout.
     * @param txLabel Transaction label.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable UUID txCoordinatorId,
            @Nullable ReplicationGroupId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable InternalTransaction tx,
            @Nullable Long initialVacuumObservationTimestamp,
            @Nullable Long cleanupCompletionTimestamp,
            @Nullable Boolean isFinishedDueToTimeout,
            @Nullable String txLabel
    ) {
        this.txState = txState;
        this.txCoordinatorId = txCoordinatorId;
        this.commitPartitionId = commitPartitionId;
        this.commitTimestamp = commitTimestamp;
        this.tx = tx;
        this.cleanupCompletionTimestamp = cleanupCompletionTimestamp;
        this.isFinishedDueToTimeout = isFinishedDueToTimeout;
        this.txLabel = txLabel;

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

        return new TxStateMetaAbandoned(txCoordinatorId, commitPartitionId, txLabel);
    }

    /**
     * Creates a transaction state for the same transaction, but this one is marked finishing.
     *
     * @return Transaction state meta.
     */
    public TxStateMetaFinishing finishing(boolean isFinishedDueToTimeoutFlag) {
        return new TxStateMetaFinishing(txCoordinatorId, commitPartitionId, isFinishedDueToTimeoutFlag, txLabel);
    }

    @Override
    public TxState txState() {
        return txState;
    }

    public @Nullable UUID txCoordinatorId() {
        return txCoordinatorId;
    }

    public @Nullable ReplicationGroupId commitPartitionId() {
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

    public @Nullable Boolean isFinishedDueToTimeout() {
        return isFinishedDueToTimeout;
    }

    public @Nullable String txLabel() {
        return txLabel;
    }

    @Override
    public TxStateMetaMessage toTransactionMetaMessage(
            ReplicaMessagesFactory replicaMessagesFactory,
            TxMessagesFactory txMessagesFactory
    ) {
        return txMessagesFactory.txStateMetaMessage()
                .txState(txState)
                .txCoordinatorId(txCoordinatorId)
                .commitPartitionId(
                        commitPartitionId == null ? null : toReplicationGroupIdMessage(replicaMessagesFactory, commitPartitionId)
                )
                .commitTimestamp(commitTimestamp)
                .initialVacuumObservationTimestamp(initialVacuumObservationTimestamp)
                .cleanupCompletionTimestamp(cleanupCompletionTimestamp)
                .isFinishedDueToTimeout(isFinishedDueToTimeout)
                .txLabel(txLabel)
                .build();
    }

    @Override
    public String toString() {
        return S.toString(TxStateMeta.class, this);
    }

    /**
     * Creates a builder for TxStateMeta.
     *
     * @param txState Transaction state.
     * @return Builder.
     */
    public static TxStateMetaBuilder builder(TxState txState) {
        return new TxStateMetaBuilder(txState);
    }

    /**
     * Creates a builder for nullable TxStateMeta.
     *
     * @param old Old meta.
     * @param txState Transaction state to use. If old meta is null, the builder will be created with this state.
     * @return Builder.
     */
    public static TxStateMetaBuilder builder(@Nullable TxStateMeta old, TxState txState) {
        return old == null ? builder(txState) : old.mutate().txState(txState);
    }

    /**
     * Mutate this meta using builder.
     *
     * @return Builder.
     */
    public TxStateMetaBuilder mutate() {
        return new TxStateMetaBuilder(this);
    }

    /**
     * Builder for TxStateMeta.
     */
    public static class TxStateMetaBuilder {
        protected TxState txState;
        protected @Nullable UUID txCoordinatorId;
        protected @Nullable ReplicationGroupId commitPartitionId;
        private @Nullable HybridTimestamp commitTimestamp;
        private @Nullable Long initialVacuumObservationTimestamp;
        private @Nullable Long cleanupCompletionTimestamp;
        protected @Nullable Boolean isFinishedDueToTimeout;
        protected @Nullable String txLabel;
        private @Nullable InternalTransaction tx;

        TxStateMetaBuilder(TxState txState) {
            this.txState = txState;
        }

        TxStateMetaBuilder(TxStateMeta old) {
            this.txState = old.txState;
            this.txCoordinatorId = old.txCoordinatorId;
            this.commitPartitionId = old.commitPartitionId;
            this.commitTimestamp = old.commitTimestamp;
            this.initialVacuumObservationTimestamp = old.initialVacuumObservationTimestamp;
            this.cleanupCompletionTimestamp = old.cleanupCompletionTimestamp;
            this.isFinishedDueToTimeout = old.isFinishedDueToTimeout;
            this.txLabel = old.txLabel;
            this.tx = old.tx;
        }

        public TxStateMetaBuilder txState(TxState txState) {
            this.txState = txState;
            return this;
        }

        /**
         * Sets transaction coordinator id. {@code null} value is ignored, because this field can change value only from {@code null}
         * to non-{@code null}, and can't change one value to another.
         *
         * @param txCoordinatorId Transaction coordinator id.
         * @return Builder.
         */
        public TxStateMetaBuilder txCoordinatorId(@Nullable UUID txCoordinatorId) {
            if (txCoordinatorId != null) {
                this.txCoordinatorId = txCoordinatorId;
            }

            return this;
        }

        /**
         * Sets commit partition replication group id. {@code null} value is ignored, because this field can change value only from
         * {@code null} to non-{@code null}, and can't change one value to another.
         *
         * @param commitPartitionId Commit partition replication group id.
         * @return Builder.
         */
        public TxStateMetaBuilder commitPartitionId(@Nullable ReplicationGroupId commitPartitionId) {
            if (commitPartitionId != null) {
                this.commitPartitionId = commitPartitionId;
            }

            return this;
        }

        /**
         * Sets commit timestamp. {@code null} value is ignored, because this field can change value only from {@code null}
         * to non-{@code null}, and can't change one value to another.
         *
         * @param commitTimestamp Commit timestamp.
         * @return Builder.
         */
        public TxStateMetaBuilder commitTimestamp(@Nullable HybridTimestamp commitTimestamp) {
            if (commitTimestamp != null) {
                this.commitTimestamp = commitTimestamp;
            }

            return this;
        }

        public TxStateMetaBuilder initialVacuumObservationTimestamp(@Nullable Long initialVacuumObservationTimestamp) {
            this.initialVacuumObservationTimestamp = initialVacuumObservationTimestamp;
            return this;
        }

        public TxStateMetaBuilder cleanupCompletionTimestamp(@Nullable Long cleanupCompletionTimestamp) {
            this.cleanupCompletionTimestamp = cleanupCompletionTimestamp;
            return this;
        }

        public TxStateMetaBuilder finishedDueToTimeout(@Nullable Boolean finishedDueToTimeout) {
            isFinishedDueToTimeout = finishedDueToTimeout;
            return this;
        }

        public TxStateMetaBuilder txLabel(@Nullable String txLabel) {
            this.txLabel = txLabel;
            return this;
        }

        public TxStateMetaBuilder tx(@Nullable InternalTransaction tx) {
            this.tx = tx;
            return this;
        }

        /**
         * Builds TxStateMeta.
         *
         * @return TxStateMeta.
         */
        public TxStateMeta build() {
            requireNonNull(txState, "txState must not be null");

            if (txState == FINISHING) {
                return new TxStateMetaFinishing(txCoordinatorId, commitPartitionId, isFinishedDueToTimeout, txLabel);
            } else if (txState == ABANDONED) {
                return new TxStateMetaAbandoned(txCoordinatorId, commitPartitionId, txLabel);
            } else {
                return new TxStateMeta(
                        txState,
                        txCoordinatorId,
                        commitPartitionId,
                        commitTimestamp,
                        tx,
                        initialVacuumObservationTimestamp,
                        cleanupCompletionTimestamp,
                        isFinishedDueToTimeout,
                        txLabel
                );
            }
        }
    }
}
