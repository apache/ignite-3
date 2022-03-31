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

package org.apache.ignite.internal.client.compute;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.network.ClusterNode;

/**
 * Client compute implementation.
 */
public class ClientCompute implements IgniteCompute {
    /** Channel. */
    private final ReliableChannel ch;

    /**
     * Constructor.
     *
     * @param ch Channel.
     */
    public ClientCompute(ReliableChannel ch) {
        this.ch = ch;
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        return execute(nodes, jobClass.getName(), args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(jobClassName);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        return ch.serviceAsync(ClientOp.COMPUTE_EXECUTE, w -> {
            // TODO: Cluster awareness (IGNITE-16771).
            ClusterNode node = randomNode(nodes);

            w.out().packString(node.id());
            w.out().packString(node.name());
            w.out().packString(node.address().host());
            w.out().packInt(node.address().port());

            w.out().packString(jobClassName);
            w.out().packObjectArray(args);
        }, r -> (R)r.in().unpackObjectWithType());
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass,
            Object... args) {
        return broadcast(nodes, jobClass.getName(), args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        // TODO: Send request to all known nodes.
        // How to match connection to node? Extend handshake.
        return null;
    }

    private ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = ThreadLocalRandom.current().nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }
}
