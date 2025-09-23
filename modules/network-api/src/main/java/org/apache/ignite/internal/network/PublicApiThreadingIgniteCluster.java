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

package org.apache.ignite.internal.network;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.thread.PublicApiThreading.execUserSyncOperation;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.IgniteCluster;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link IgniteCluster} that maintains public API invariants relating to threading.
 * That is, it adds protection against thread hijacking by users and also marks threads as 'executing a sync user operation' or
 * 'executing an async user operation'.
 */
public class PublicApiThreadingIgniteCluster implements IgniteCluster, Wrapper {
    private final IgniteCluster cluster;

    public PublicApiThreadingIgniteCluster(IgniteCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(cluster);
    }

    @Override
    public UUID id() {
        return execUserSyncOperation(cluster::id);
    }

    @Override
    public Collection<ClusterNode> nodes() {
        return execUserSyncOperation(cluster::nodes);
    }

    @Override
    public CompletableFuture<Collection<ClusterNode>> nodesAsync() {
        return completedFuture(nodes());
    }

    @Override
    public @Nullable ClusterNode localNode() {
        return execUserSyncOperation(cluster::localNode);
    }
}
