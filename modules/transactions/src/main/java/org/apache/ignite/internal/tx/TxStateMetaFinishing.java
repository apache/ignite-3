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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateMetaMessage;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TxStateMeta} implementation for {@link TxState#FINISHING} state. Contains future that is completed after the state of
 * corresponding transaction changes to final state.
 */
public class TxStateMetaFinishing extends TxStateMeta {
    private static final long serialVersionUID = 9122953981654023665L;

    /** Future that is completed after the state of corresponding transaction changes to final state. */
    private final CompletableFuture<TransactionMeta> txFinishFuture = new CompletableFuture<>();

    /**
     * Constructor.
     *
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition id.
     * @param isFinishingDueToTimeout {@code true} if transaction is finishing due to timeout, {@code false} otherwise.
     * @param txLabel Transaction label.
     */
    public TxStateMetaFinishing(
            @Nullable UUID txCoordinatorId,
            @Nullable ReplicationGroupId commitPartitionId,
            @Nullable Boolean isFinishingDueToTimeout,
            @Nullable String txLabel
    ) {
        super(TxState.FINISHING, txCoordinatorId, commitPartitionId, null, null, null, null, isFinishingDueToTimeout, txLabel);
    }

    /**
     * Future that is completed after the state of corresponding transaction changes to final state.
     *
     * @return Future that is completed after the state of corresponding transaction changes to final state.
     */
    public CompletableFuture<TransactionMeta> txFinishFuture() {
        return txFinishFuture;
    }

    @Override
    public @Nullable HybridTimestamp commitTimestamp() {
        throw new UnsupportedOperationException("Can't get commit timestamp from FINISHING transaction state meta.");
    }

    @Override
    public TxStateMetaMessage toTransactionMetaMessage(
            ReplicaMessagesFactory replicaMessagesFactory,
            TxMessagesFactory txMessagesFactory
    ) {
        throw new AssertionError("This state shouldn't be transferred over the network.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        TxStateMetaFinishing that = (TxStateMetaFinishing) o;

        return txFinishFuture.equals(that.txFinishFuture);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + txFinishFuture.hashCode();

        return result;
    }

    @Override
    public TxStateMetaBuilder mutate() {
        return new TxStateMetaFinishingBuilder(this);
    }

    /**
     * Builder for {@link TxStateMetaAbandoned} instances.
     */
    public static class TxStateMetaFinishingBuilder extends TxStateMetaBuilder {
        TxStateMetaFinishingBuilder(TxStateMeta old) {
            super(old);
        }

        @Override
        public TxStateMeta build() {
            if (txState == TxState.FINISHING) {
                return new TxStateMetaFinishing(txCoordinatorId, commitPartitionId, isFinishedDueToTimeout, txLabel);
            } else {
                return super.build();
            }
        }
    }
}
