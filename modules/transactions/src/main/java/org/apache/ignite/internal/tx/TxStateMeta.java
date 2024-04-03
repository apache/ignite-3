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

import static org.apache.ignite.internal.tx.TxState.ABANDONED;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;

import java.util.Objects;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction state meta.
 */
public class TxStateMeta implements TransactionMeta {
    private static final long serialVersionUID = 8521181896862227127L;

    private final TxState txState;

    private final String txCoordinatorId;

    /** Identifier of the replication group that manages a transaction state. */
    private final TablePartitionId commitPartitionId;

    private final HybridTimestamp commitTimestamp;

    private final Long initialVacuumObservationTimestamp;

    @Nullable
    private final Long cleanupCompletionTimestamp;

    /**
     * Constructor.
     *
     * @param txState Transaction state.
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition replication group id.
     * @param commitTimestamp Commit timestamp.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable String txCoordinatorId,
            @Nullable TablePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        this(txState, txCoordinatorId, commitPartitionId, commitTimestamp, null, null);
    }

    /**
     * Constructor.
     *
     * @param txState Transaction state.
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition replication group id.
     * @param commitTimestamp Commit timestamp.
     * @param initialVacuumObservationTimestamp Initial vacuum observation timestamp.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable String txCoordinatorId,
            @Nullable TablePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable Long initialVacuumObservationTimestamp
    ) {
        this(txState, txCoordinatorId, commitPartitionId, commitTimestamp, initialVacuumObservationTimestamp, null);
    }

    /**
     * Constructor.
     *
     * @param txState Transaction state.
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition replication group id.
     * @param commitTimestamp Commit timestamp.
     * @param initialVacuumObservationTimestamp Initial vacuum observation timestamp.
     * @param cleanupCompletionTimestamp Cleanup completion timestamp.
     */
    public TxStateMeta(
            TxState txState,
            @Nullable String txCoordinatorId,
            @Nullable TablePartitionId commitPartitionId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable Long initialVacuumObservationTimestamp,
            @Nullable Long cleanupCompletionTimestamp
    ) {
        this.txState = txState;
        this.txCoordinatorId = txCoordinatorId;
        this.commitPartitionId = commitPartitionId;
        this.commitTimestamp = commitTimestamp;
        this.initialVacuumObservationTimestamp = initialVacuumObservationTimestamp;
        this.cleanupCompletionTimestamp = cleanupCompletionTimestamp;
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

    public @Nullable String txCoordinatorId() {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TxStateMeta that = (TxStateMeta) o;

        if (txState != that.txState) {
            return false;
        }

        if (txCoordinatorId != null ? !txCoordinatorId.equals(that.txCoordinatorId) : that.txCoordinatorId != null) {
            return false;
        }

        if (commitPartitionId != null ? !commitPartitionId.equals(that.commitPartitionId) : that.commitPartitionId != null) {
            return false;
        }

        if (commitTimestamp != null ? !commitTimestamp.equals(that.commitTimestamp) : that.commitTimestamp != null) {
            return false;
        }

        if (initialVacuumObservationTimestamp != null
                ? !initialVacuumObservationTimestamp.equals(that.initialVacuumObservationTimestamp)
                : that.initialVacuumObservationTimestamp != null
        ) {
            return false;
        }

        return cleanupCompletionTimestamp != null
                ? cleanupCompletionTimestamp.equals(that.cleanupCompletionTimestamp)
                : that.cleanupCompletionTimestamp == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(txState, txCoordinatorId, commitPartitionId, commitTimestamp, initialVacuumObservationTimestamp,
                cleanupCompletionTimestamp);
    }

    @Override
    public String toString() {
        return S.toString(TxStateMeta.class, this);
    }
}
