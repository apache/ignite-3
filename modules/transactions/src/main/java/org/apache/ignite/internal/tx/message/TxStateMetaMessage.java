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

package org.apache.ignite.internal.tx.message;

import java.util.UUID;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.jetbrains.annotations.Nullable;

/** Message for transferring a {@link TxStateMeta}. */
@Transferable(TxMessageGroup.TX_STATE_META_MESSAGE)
public interface TxStateMetaMessage extends TransactionMetaMessage {
    /** Transaction coordinator ID. */
    @Nullable UUID txCoordinatorId();

    /** ID of the replication group that manages a transaction state. */
    @Nullable ZonePartitionIdMessage commitPartitionId();

    /** Initial vacuum observation timestamp. */
    @Nullable Long initialVacuumObservationTimestamp();

    /** Cleanup completion timestamp. */
    @Nullable Long cleanupCompletionTimestamp();

    @Nullable Boolean isFinishedDueToTimeout();

    @Nullable String txLabel();

    /** Converts to {@link TxStateMeta}. */
    default TxStateMeta asTxStateMeta() {
        ZonePartitionIdMessage commitPartitionId = commitPartitionId();

        return new TxStateMeta(
                txState(),
                txCoordinatorId(),
                commitPartitionId == null ? null : commitPartitionId.asReplicationGroupId(),
                commitTimestamp(),
                null,
                initialVacuumObservationTimestamp(),
                cleanupCompletionTimestamp(),
                isFinishedDueToTimeout(),
                txLabel()
        );
    }

    @Override
    default TransactionMeta asTransactionMeta() {
        return asTxStateMeta();
    }
}
