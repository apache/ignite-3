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

package org.apache.ignite.internal.replicator;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.jetbrains.annotations.TestOnly;

/** Utilities for working with replicas and replicas manager in tests. */
public final class ReplicaTestUtils {

    /**
     * Returns raft-client if exists.
     *
     * @param node Ignite node that hosts the raft-client.
     * @param tableId Desired table's ID.
     * @param partId Desired partition's ID.
     *
     * @return Optional with raft-client if exists on the node by given identifiers.
     */
    @TestOnly
    public static Optional<RaftGroupService> getRaftClient(Ignite node, int tableId, int partId) {
        CompletableFuture<Replica> replicaFut = getReplicaManager(node)
                .replica(new TablePartitionId(tableId, partId));

        if  (replicaFut == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(replicaFut.get(15, TimeUnit.SECONDS).raftClient());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            return Optional.empty();
        }
    }

    /**
     * Extracts {@link ReplicaManager} from the given {@link Ignite} node.
     *
     * @param node The given node with desired replica manager.
     *
     * @return Replica manager component from given node.
     */
    @TestOnly
    public static ReplicaManager getReplicaManager(Ignite node) {
        return IgniteTestUtils.getFieldValue(node, "replicaMgr");
    }
}
