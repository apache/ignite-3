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

package org.apache.ignite.internal.cluster.management;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorageManager;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Used to handle volatile information about cluster ID used to restrict which nodes can connect this one and vice versa.
 */
public class ClusterIdService extends ClusterIdHolder implements IgniteComponent {
    private final ClusterStateStorage clusterStateStorage;

    public ClusterIdService(ClusterStateStorage clusterStateStorage) {
        this.clusterStateStorage = clusterStateStorage;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        var clusterStateManager = new ClusterStateStorageManager(clusterStateStorage);

        ClusterState clusterState = clusterStateManager.getClusterState();
        if (clusterState != null) {
            clusterId(clusterState.clusterTag().clusterId());
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }
}
