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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction finish replica request that will trigger following actions processing.
 *
 *  <ol>
 *      <li>Evaluate commit timestamp.</li>
 *      <li>Run specific raft {@code FinishTxCommand} command, that will apply txn state to corresponding txStateStorage.</li>
 *      <li>Send cleanup requests to all enlisted primary replicas.</li>
 *  </ol>
 */
@Transferable(TxMessageGroup.TX_FINISH_REQUEST)
public interface TxFinishReplicaRequest extends PrimaryReplicaRequest, TimestampAware {
    /**
     * Returns transaction Id.
     *
     * @return Transaction id.
     */
    UUID txId();

    TablePartitionIdMessage commitPartitionId();

    /**
     * Returns {@code True} if a commit request.
     *
     * @return {@code True} to commit.
     */
    boolean commit();

    /** Transaction commit timestamp. */
    @Nullable HybridTimestamp commitTimestamp();

    /** Enlisted partition groups aggregated by expected primary replica nodes. */
    Map<TablePartitionIdMessage, String> groups();
}
