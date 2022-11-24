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

package org.apache.ignite.internal.table.distributed.replicator;

import java.io.Serializable;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxStateReplicaRequest;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Response for the {@link TxStateReplicaRequest}. Can contain either the Partition Group leader, which should be
 * queried for the TX Meta, or the TX Meta itself.
 */
public class LeaderOrTxState implements Serializable {
    private static final long serialVersionUID = -3555591755828355117L;

    @Nullable
    private final ClusterNode leader;

    @Nullable
    private final TxMeta txMeta;

    /**
     * Creates a response.
     *
     * @param leader Leader node.
     * @param txMeta TX meta.
     */
    public LeaderOrTxState(@Nullable ClusterNode leader, @Nullable TxMeta txMeta) {
        this.leader = leader;
        this.txMeta = txMeta;
    }

    public @Nullable ClusterNode leader() {
        return leader;
    }

    public @Nullable TxMeta txMeta() {
        return txMeta;
    }
}
