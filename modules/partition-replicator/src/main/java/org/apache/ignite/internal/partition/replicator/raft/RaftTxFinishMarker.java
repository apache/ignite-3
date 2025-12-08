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

package org.apache.ignite.internal.partition.replicator.raft;

import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.jetbrains.annotations.Nullable;

/**
 * Contains transaction finishing logic for partitions Raft listener implementations.
 */
public class RaftTxFinishMarker {
    private final TxManager txManager;

    public RaftTxFinishMarker(TxManager txManager) {
        this.txManager = txManager;
    }

    /**
     * Marks the transaction as finished in local tx state map.
     *
     * @param txId Transaction id.
     * @param commit Whether this is a commit.
     * @param commitTimestamp Commit timestamp ({@code null} when aborting).
     * @param commitPartitionId Commit partition ID.
     */
    public void markFinished(
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable ZonePartitionId commitPartitionId
    ) {
        txManager.updateTxMeta(txId, old -> TxStateMeta.builder(old, commit ? COMMITTED : ABORTED)
                .commitPartitionId(commitPartitionId)
                .commitTimestamp(commit ? commitTimestamp : null)
                .build()
        );
    }
}
