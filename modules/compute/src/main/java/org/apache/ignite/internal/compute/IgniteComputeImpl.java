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

package org.apache.ignite.internal.compute;

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteCompute {
    private final TopologyService topologyService;
    private final ComputeComponent computeComponent;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public IgniteComputeImpl(TopologyService topologyService, ComputeComponent computeComponent) {
        this.topologyService = topologyService;
        this.computeComponent = computeComponent;
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        return executeOnOneNode(randomNode(nodes), jobClass, args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        return executeOnOneNode(randomNode(nodes), jobClassName, args);
    }

    private ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = random.nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private <R> CompletableFuture<R> executeOnOneNode(ClusterNode targetNode, Class<? extends ComputeJob<R>> jobClass, Object[] args) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(jobClass, args);
        } else {
            return computeComponent.executeRemotely(targetNode, jobClass, args);
        }
    }

    private <R> CompletableFuture<R> executeOnOneNode(ClusterNode targetNode, String jobClassName, Object[] args) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(jobClassName, args);
        } else {
            return computeComponent.executeRemotely(targetNode, jobClassName, args);
        }
    }

    private boolean isLocal(ClusterNode targetNode) {
        return targetNode.equals(topologyService.localMember());
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(
            Set<ClusterNode> nodes,
            Class<? extends ComputeJob<R>> jobClass,
            Object... args
    ) {
        return nodes.stream()
                .collect(toUnmodifiableMap(node -> node, node -> executeOnOneNode(node, jobClass, args)));
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        return nodes.stream()
                .collect(toUnmodifiableMap(node -> node, node -> executeOnOneNode(node, jobClassName, args)));
    }
}
