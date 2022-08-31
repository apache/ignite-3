/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx.message;

import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.annotations.Marshallable;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Transaction finish replica request that will trigger following actions processing.
 *
 *  <ol>
 *      <li>Evaluate commit timestamp.</li>
 *      <li>Run specific raft {@code FinishTxCommand} command, that will apply txn state to corresponding txStateStorage.</li>
 *      <li>Send cleanup requests to all enlisted primary replicas.</li>
 *  </ol>
 */
@Transferable(value = TxMessageGroup.TX_FINISH_REQUEST)
public interface TxFinishReplicaRequest extends ReplicaRequest {
    /**
     * Returns transaction Id.
     *
     * @return Transaction id.
     */
    @Marshallable
    UUID txId();

    /**
     * Returns {@code True} if a commit request.
     *
     * @return {@code True} to commit.
     */
    boolean commit();

    /**
     * Returns enlisted partition groups aggregated by expected primary replica nodes.
     *
     * @return Enlisted partition groups aggregated by expected primary replica nodes.
     */
    @Marshallable
    TreeMap<ClusterNode, List<String>> groups();
}
