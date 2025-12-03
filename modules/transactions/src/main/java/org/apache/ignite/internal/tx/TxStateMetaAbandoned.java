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

import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toReplicationGroupIdMessage;
import static org.apache.ignite.internal.tx.TxState.ABANDONED;

import java.util.UUID;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateMetaAbandonedMessage;
import org.apache.ignite.internal.util.FastTimestamps;
import org.jetbrains.annotations.Nullable;

/**
 * Abandoned transaction state meta.
 */
public class TxStateMetaAbandoned extends TxStateMeta {
    private static final long serialVersionUID = 8521181896862227127L;

    /** Timestamp when the latest {@code ABANDONED} state set. */
    private final long lastAbandonedMarkerTs;

    /**
     * Constructor.
     *
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition replication group ID.
     * @param txLabel Transaction label.
     */
    public TxStateMetaAbandoned(
            UUID txCoordinatorId,
            ReplicationGroupId commitPartitionId,
            @Nullable String txLabel
    ) {
        super(ABANDONED, txCoordinatorId, commitPartitionId, null, null, null, null, null, txLabel);

        this.lastAbandonedMarkerTs = FastTimestamps.coarseCurrentTimeMillis();
    }

    /**
     * The last timestamp when the transaction was marked as abandoned.
     *
     * @return Timestamp or {@code 0} if the transaction is in not abandoned.
     */
    public long lastAbandonedMarkerTs() {
        return lastAbandonedMarkerTs;
    }

    @Override
    public TxStateMetaAbandonedMessage toTransactionMetaMessage(
            ReplicaMessagesFactory replicaMessagesFactory,
            TxMessagesFactory txMessagesFactory
    ) {
        ReplicationGroupId commitPartitionId = commitPartitionId();

        return txMessagesFactory.txStateMetaAbandonedMessage()
                .txState(txState())
                .txCoordinatorId(txCoordinatorId())
                .commitPartitionId(
                        commitPartitionId == null ? null : toReplicationGroupIdMessage(replicaMessagesFactory, commitPartitionId)
                )
                .commitTimestamp(commitTimestamp())
                .initialVacuumObservationTimestamp(initialVacuumObservationTimestamp())
                .cleanupCompletionTimestamp(cleanupCompletionTimestamp())
                .lastAbandonedMarkerTs(lastAbandonedMarkerTs)
                .txLabel(txLabel())
                .build();
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

        TxStateMetaAbandoned that = (TxStateMetaAbandoned) o;

        return lastAbandonedMarkerTs == that.lastAbandonedMarkerTs;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + Long.hashCode(lastAbandonedMarkerTs);

        return result;
    }

    @Override
    public String toString() {
        return S.toString(TxStateMetaAbandoned.class, this);
    }

    @Override
    public TxStateMetaBuilder mutate() {
        return new TxStateMetaAbandonedBuilder(this);
    }

    /**
     * Builder for {@link TxStateMetaAbandoned} instances.
     */
    public static class TxStateMetaAbandonedBuilder extends TxStateMetaBuilder {
        private long lastAbandonedMarkerTs;

        TxStateMetaAbandonedBuilder(TxStateMeta old) {
            super(old);
        }

        public TxStateMetaAbandonedBuilder lastAbandonedMarkerTs(long lastAbandonedMarkerTs) {
            this.lastAbandonedMarkerTs = lastAbandonedMarkerTs;
            return this;
        }

        @Override
        public TxStateMeta build() {
            return new TxStateMetaAbandoned(txCoordinatorId, commitPartitionId, txLabel);
        }
    }
}
