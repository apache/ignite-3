/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.handler.requests.compute;

import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;

/**
 * Compute execute request.
 */
public class ClientComputeExecuteRequest {
    /**
     * Processes the request.
     *
     * @param in        Unpacker.
     * @param out       Packer.
     * @param compute   Compute.
     * @param cluster   Cluster.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteCompute compute,
            ClusterService cluster) {
        int nodeCnt = in.unpackArrayHeader();
        var nodes = new HashSet<ClusterNode>(nodeCnt);

        for (int i = 0; i < nodeCnt; i++) {
            var node = new ClusterNode(in.unpackString(), in.unpackString(), new NetworkAddress(in.unpackString(), in.unpackInt()));
            nodes.add(node);
        }

        String jobClassName = in.unpackString();

        int argCnt = in.unpackArrayHeader();
        Object[] args = argCnt == 0 ? OBJECT_EMPTY_ARRAY : new Object[argCnt];

        for (int i = 0; i < argCnt; i++) {
            args[i] = in.unpackObjectWithType();
        }

        return compute.execute(nodes, jobClassName, args).thenAccept(out::packObjectWithType);
    }
}
