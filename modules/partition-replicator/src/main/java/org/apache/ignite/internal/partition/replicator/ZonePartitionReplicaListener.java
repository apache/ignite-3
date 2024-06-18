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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRowImpl;

/**
 * Zone partition replica listener.
 */
public class ZonePartitionReplicaListener implements ReplicaListener {

    @Override
    public CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, String senderId) {
        var res = new BinaryRowImpl(
                1,
                new BinaryTupleBuilder(2).appendLong(1).appendInt(-1).build());
        return CompletableFuture.completedFuture(new ReplicaResult(
                res,
                CompletableFuture.completedFuture(res)));
    }

    @Override
    public RaftCommandRunner raftClient() {
        throw new UnsupportedOperationException("Raft client is not defined in ZoneReplicaListener");
    }
}
