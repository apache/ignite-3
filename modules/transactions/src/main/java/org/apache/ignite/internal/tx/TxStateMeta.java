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
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.UNKNOWN;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.tx.TxStateMetaUnknown.txStateMetaUnknown;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.util.ExceptionUtils;
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
    private final @Nullable ZonePartitionId commitPartitionId;

    private final @Nullable HybridTimestamp commitTimestamp;

    private final @Nullable Long initialVacuumObservationTimestamp;

    private final @Nullable Long cleanupCompletionTimestamp;

    private final @Nullable Boolean isFinishedDueToTimeout;

    private final @Nullable String txLabel;

    /** Information about exceptional transaction abortion (primary entry with suppressed exceptions, may be {@code null}). */
    private final transient @Nullable Throwable exceptionInfo;

    /**
     * The ignite transaction object is associated with this state. This field can be initialized only on the transaction coordinator,
     * {@code null} on the other nodes. Moreover, this field can be set to {@code null} even on the transaction coordinator under certain
     * circumstances.
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
            @Nullable ZonePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable InternalTransaction tx,
            @Nullable Boolean isFinishedDueToTimeout
    ) {
        this(txState, txCoordinatorId, commitPartitionId, commitTimestamp, tx, null, null, isFinishedDueToTimeout, null, null);
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
            @Nullable ZonePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable InternalTransaction tx,
            @Nullable Long initialVacuumObservationTimestamp,
            @Nullable Long cleanupCompletionTimestamp,
            @Nullable Boolean isFinishedDueToTimeout,
            @Nullable String txLabel
    ) {
        this(
                txState,
                txCoordinatorId,
                commitPartitionId,
                commitTimestamp,
                tx,
                initialVacuumObservationTimestamp,
                cleanupCompletionTimestamp,
                isFinishedDueToTimeout,
                txLabel,
                null
        );
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
     * @param exceptionInfo Exception info for exceptional abort.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable UUID txCoordinatorId,
            @Nullable ZonePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable InternalTransaction tx,
            @Nullable Long initialVacuumObservationTimestamp,
            @Nullable Long cleanupCompletionTimestamp,
            @Nullable Boolean isFinishedDueToTimeout,
            @Nullable String txLabel,
            @Nullable Throwable exceptionInfo
    ) {
        this.txState = txState;
        this.txCoordinatorId = txCoordinatorId;
        this.commitPartitionId = commitPartitionId;
        this.commitTimestamp = commitTimestamp;
        this.tx = tx;
        this.cleanupCompletionTimestamp = cleanupCompletionTimestamp;
        this.isFinishedDueToTimeout = isFinishedDueToTimeout;
        this.txLabel = txLabel;
        this.exceptionInfo = exceptionInfo;

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

        return new TxStateMetaAbandoned(txCoordinatorId, commitPartitionId, tx, txLabel);
    }

    /**
     * Creates a transaction state for the same transaction, but this one is marked finishing.
     *
     * @return Transaction state meta.
     */
    public TxStateMetaFinishing finishing(boolean isFinishedDueToTimeoutFlag) {
        return new TxStateMetaFinishing(txCoordinatorId, commitPartitionId, isFinishedDueToTimeoutFlag, txLabel, exceptionInfo);
    }

    @Override
    public TxState txState() {
        return txState;
    }

    public @Nullable UUID txCoordinatorId() {
        return txCoordinatorId;
    }

    public @Nullable ZonePartitionId commitPartitionId() {
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

    public @Nullable Throwable exceptionInfo() {
        return exceptionInfo;
    }

    /**
     * Records exceptional information by mutating tx state or by creating a new one. This method should be called after tx is finished.
     *
     * @param old previous TxStateMeta.
     * @param throwable to record
     */
    public static TxStateMeta recordExceptionInfo(@Nullable TxStateMeta old, Throwable throwable) {
        Throwable normalized = normalizeThrowable(throwable);
        return old == null
                ? builder(old, ABORTED).exceptionInfo(normalized).build()
                : old.mutate().exceptionInfo(normalized).build();
    }

    /**
     * Returns the stored exception or {@code null} if nothing usable is present.
     *
     * @param exceptionInfo Exception info.
     * @return Throwable or {@code null} if nothing usable is present.
     */
    public static @Nullable Throwable aggregateExceptionInfo(@Nullable Throwable exceptionInfo) {
        return exceptionInfo;
    }

    private static @Nullable Throwable normalizeThrowable(@Nullable Throwable throwable) {
        return throwable == null ? null : ExceptionUtils.unwrapRootCause(throwable);
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
                        commitPartitionId == null ? null : toZonePartitionIdMessage(replicaMessagesFactory, commitPartitionId)
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
        protected @Nullable ZonePartitionId commitPartitionId;
        private @Nullable HybridTimestamp commitTimestamp;
        private @Nullable Long initialVacuumObservationTimestamp;
        private @Nullable Long cleanupCompletionTimestamp;
        protected @Nullable Boolean isFinishedDueToTimeout;
        protected @Nullable String txLabel;
        protected @Nullable InternalTransaction tx;
        protected @Nullable Throwable exceptionInfo;

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
            this.exceptionInfo = old.exceptionInfo;
        }

        public TxStateMetaBuilder txState(TxState txState) {
            this.txState = txState;
            return this;
        }

        /**
         * Sets transaction coordinator id. {@code null} value is ignored, because this field can change value only from {@code null} to
         * non-{@code null}, and can't change one value to another.
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
        public TxStateMetaBuilder commitPartitionId(@Nullable ZonePartitionId commitPartitionId) {
            if (commitPartitionId != null) {
                this.commitPartitionId = commitPartitionId;
            }

            return this;
        }

        /**
         * Sets commit timestamp. {@code null} value is ignored, because this field can change value only from {@code null} to
         * non-{@code null}, and can't change one value to another.
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
         * Records exception info as a single primary entry. Any previously recorded exception is attached as suppressed.
         *
         * @param exceptionInfo Exception info to record.
         * @return Builder.
         */
        public TxStateMetaBuilder exceptionInfo(@Nullable Throwable exceptionInfo) {
            if (exceptionInfo != null) {
                Throwable normalized = normalizeThrowable(exceptionInfo);
                if (normalized == null) {
                    return this;
                }

                if (this.exceptionInfo == null) {
                    this.exceptionInfo = normalized;
                } else {
                    normalized.addSuppressed(this.exceptionInfo);
                    this.exceptionInfo = normalized;
                }
            }
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
                return new TxStateMetaFinishing(txCoordinatorId, commitPartitionId, isFinishedDueToTimeout, txLabel, exceptionInfo);
            } else if (txState == ABANDONED) {
                return new TxStateMetaAbandoned(txCoordinatorId, commitPartitionId, tx, txLabel, exceptionInfo);
            } else if (txState == UNKNOWN) {
                return txStateMetaUnknown();
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
                        txLabel,
                        exceptionInfo
                );
            }
        }
    }
}
