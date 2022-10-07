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

package org.apache.ignite.client.handler.requests.compute;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.NotNull;

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
        var nodeName = in.tryUnpackNil() ? null : in.unpackString();

        var node = nodeName == null
                ? cluster.topologyService().localMember()
                : cluster.topologyService().getByConsistentId(nodeName);

        if (node == null) {
            throw new IgniteException("Specified node is not present in the cluster: " + nodeName);
        }

        String jobClassName = in.unpackString();

        Object[] args = unpackArgs(in);

        return compute.execute(Set.of(node), jobClassName, args).thenAccept(out::packObjectAsBinaryTuple);
    }

    /**
     * Unpacks args.
     *
     * @param in Unpacker.
     * @return Args array.
     */
    @NotNull
    public static Object[] unpackArgs(ClientMessageUnpacker in) {
        return in.unpackObjectArrayFromBinaryTuple();
    }
}
