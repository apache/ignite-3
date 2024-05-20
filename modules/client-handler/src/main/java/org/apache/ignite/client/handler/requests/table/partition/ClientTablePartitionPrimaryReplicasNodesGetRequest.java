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

package org.apache.ignite.client.handler.requests.table.partition;

import static org.apache.ignite.client.handler.requests.cluster.ClientClusterGetNodesRequest.packClusterNode;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTableAsync;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.requests.table.ClientTablePartitionPrimaryReplicasGetRequest;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.table.partition.Partition;

/**
 * Client primary replicas with node info retrieval request.
 * See also {@link ClientTablePartitionPrimaryReplicasGetRequest}.
 */
public class ClientTablePartitionPrimaryReplicasNodesGetRequest {

    /**
     * Process the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param tables Ignite tables.
     * @return Future which will complete after request process finished.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTables tables
    ) {
        return readTableAsync(in, tables).thenCompose(table -> {
            if (table == null) {
                out.packNil();
                return nullCompletedFuture();
            } else {
                return table.partitionManager()
                        .primaryReplicasAsync()
                        .thenAccept(partitions -> {
                            out.packInt(partitions.size());
                            for (Entry<Partition, ClusterNode> e : partitions.entrySet()) {
                                HashPartition partition = (HashPartition) e.getKey();

                                out.packInt(partition.partitionId());

                                packClusterNode(e.getValue(), out);
                            }
                        });
            }
        });
    }
}
