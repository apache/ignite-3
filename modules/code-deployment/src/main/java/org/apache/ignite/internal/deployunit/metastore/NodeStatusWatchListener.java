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

package org.apache.ignite.internal.deployunit.metastore;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.ignite.internal.deployunit.metastore.status.NodeStatusKey;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Node statuses store watch listener.
 */
public class NodeStatusWatchListener implements WatchListener {
    private static final IgniteLogger LOG = Loggers.forClass(NodeStatusWatchListener.class);

    private final DeploymentUnitStore deploymentUnitStore;

    private final Supplier<String> localNodeProvider;

    private final NodeEventCallback listener;

    private final ExecutorService executor = Executors.newFixedThreadPool(
            4, new NamedThreadFactory("NodeStatusWatchListener-pool", LOG));

    /**
     * Constructor.
     *
     * @param deploymentUnitStore Unit statuses store.
     * @param localNodeProvider Node consistent identifier provider.
     * @param callback Node event callback.
     */
    public NodeStatusWatchListener(DeploymentUnitStore deploymentUnitStore,
            Supplier<String> localNodeProvider,
            NodeEventCallback callback) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.localNodeProvider = localNodeProvider;
        this.listener = callback;
    }

    @Override
    public CompletableFuture<Void> onUpdate(WatchEvent event) {
        for (EntryEvent e : event.entryEvents()) {
            Entry entry = e.newEntry();

            byte[] key = entry.key();
            byte[] value = entry.value();

            NodeStatusKey nodeStatusKey = NodeStatusKey.fromBytes(key);

            if (!Objects.equals(localNodeProvider.get(), nodeStatusKey.nodeId())
                    || value == null) {
                continue;
            }

            UnitNodeStatus nodeStatus = UnitNodeStatus.deserialize(value);

            CompletableFuture.supplyAsync(() -> nodeStatus, executor)
                    .thenComposeAsync(status -> deploymentUnitStore.getAllNodes(status.id(), status.version()), executor)
                    .thenAccept(nodes -> listener.onUpdate(nodeStatus, new HashSet<>(nodes)));
        }
        return completedFuture(null);
    }

    @Override
    public void onError(Throwable e) {
        LOG.warn("Failed to process metastore deployment unit event. ", e);
    }
}
