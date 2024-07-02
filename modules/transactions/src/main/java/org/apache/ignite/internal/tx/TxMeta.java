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

import static java.util.Collections.unmodifiableCollection;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxMetaMessage;
import org.jetbrains.annotations.Nullable;

/** Transaction meta. */
public class TxMeta implements TransactionMeta {
    /** Serial version UID. */
    private static final long serialVersionUID = -172513482743911860L;

    /** Tx state. */
    private final TxState txState;

    /** Collection of enlisted partition groups. */
    private final Collection<TablePartitionId> enlistedPartitions;

    /** Commit timestamp. */
    private final @Nullable HybridTimestamp commitTimestamp;

    /**
     * The constructor.
     *
     * @param txState Tx state.
     * @param enlistedPartitions Collection of enlisted partition groups.
     * @param commitTimestamp Commit timestamp.
     */
    public TxMeta(TxState txState, Collection<TablePartitionId> enlistedPartitions, @Nullable HybridTimestamp commitTimestamp) {
        this.txState = txState;
        this.enlistedPartitions = enlistedPartitions;
        this.commitTimestamp = commitTimestamp;
    }

    @Override
    public TxState txState() {
        return txState;
    }

    public Collection<TablePartitionId> enlistedPartitions() {
        return unmodifiableCollection(enlistedPartitions);
    }

    @Override
    public @Nullable HybridTimestamp commitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public TxMetaMessage toTransactionMetaMessage(ReplicaMessagesFactory replicaMessagesFactory, TxMessagesFactory txMessagesFactory) {
        var enlistedPartitionMessages = new ArrayList<TablePartitionIdMessage>(enlistedPartitions.size());

        for (TablePartitionId enlistedPartition : enlistedPartitions) {
            enlistedPartitionMessages.add(toTablePartitionIdMessage(replicaMessagesFactory, enlistedPartition));
        }

        return txMessagesFactory.txMetaMessage()
                .txStateInt(txState.ordinal())
                .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                .enlistedPartitions(enlistedPartitionMessages)
                .build();
    }

    @Override
    public String toString() {
        return S.toString(TxMeta.class, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TxMeta other = (TxMeta) o;

        return txState == other.txState
                && enlistedPartitions.equals(other.enlistedPartitions)
                && Objects.equals(commitTimestamp, other.commitTimestamp);
    }

    @Override
    public int hashCode() {
        int result = txState.hashCode();
        result = 31 * result + enlistedPartitions.hashCode();
        result = 31 * result + (commitTimestamp != null ? commitTimestamp.hashCode() : 0);
        return result;
    }
}
