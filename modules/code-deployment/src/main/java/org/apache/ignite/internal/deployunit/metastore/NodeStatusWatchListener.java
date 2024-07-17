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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private final String nodeName;

    private final NodeEventCallback callback;

    private final ExecutorService executor;

    /**
     * Constructor.
     *
     * @param deploymentUnitStore Unit statuses store.
     * @param nodeName Node consistent ID.
     * @param callback Node event callback.
     */
    public NodeStatusWatchListener(DeploymentUnitStore deploymentUnitStore, String nodeName, NodeEventCallback callback) {
        this.deploymentUnitStore = deploymentUnitStore;
        this.nodeName = nodeName;
        this.callback = callback;

        executor = Executors.newFixedThreadPool(
                4, NamedThreadFactory.create(nodeName, "NodeStatusWatchListener-pool", LOG)
        );
    }

    @Override
    public CompletableFuture<Void> onUpdate(WatchEvent event) {
        for (EntryEvent e : event.entryEvents()) {
            Entry entry = e.newEntry();

            byte[] key = entry.key();
            byte[] value = entry.value();

            NodeStatusKey nodeStatusKey = NodeStatusKey.fromBytes(key);

            if (!Objects.equals(nodeName, nodeStatusKey.nodeId()) || value == null) {
                continue;
            }

            UnitNodeStatus nodeStatus = UnitNodeStatus.deserialize(value);

            CompletableFuture.supplyAsync(() -> nodeStatus, executor)
                    .thenComposeAsync(status -> deploymentUnitStore.getAllNodeStatuses(status.id(), status.version()), executor)
                    .thenAccept(nodes -> callback.onUpdate(nodeStatus, nodes));
        }
        return nullCompletedFuture();
    }

    @Override
    public void onError(Throwable e) {
        LOG.warn("Failed to process metastore deployment unit event. ", e);
    }

    public void stop() {
        executor.shutdown();
    }
}
