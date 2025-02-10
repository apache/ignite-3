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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.isFinalState;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.jetbrains.annotations.Nullable;

/**
 * Contains transaction finishing logic for partitions' {@link ReplicaListener} implementations.
 */
public class ReplicaTxFinishMarker {
    private final TxManager txManager;

    public ReplicaTxFinishMarker(TxManager txManager) {
        this.txManager = txManager;
    }

    /**
     * Marks the transaction as finished in local tx state map.
     *
     * @param txId Transaction id.
     * @param txState Transaction state, must be either {@link TxState#COMMITTED} or {@link TxState#ABORTED}.
     * @param commitTimestamp Commit timestamp ({@code null} when aborting).
     */
    public void markFinished(UUID txId, TxState txState, @Nullable HybridTimestamp commitTimestamp) {
        assert isFinalState(txState) : "Unexpected state [txId=" + txId + ", txState=" + txState + ']';

        txManager.updateTxMeta(txId, old -> new TxStateMeta(
                txState,
                old == null ? null : old.txCoordinatorId(),
                old == null ? null : old.commitPartitionId(),
                txState == COMMITTED ? commitTimestamp : null,
                old == null ? null : old.tx(),
                old == null ? null : old.initialVacuumObservationTimestamp(),
                old == null ? null : old.cleanupCompletionTimestamp()
        ));
    }
}
