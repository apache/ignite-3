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

package org.apache.ignite.internal.cli;

import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.rest.client.model.ClusterNode;

/**
 * Registry to get a node URL by a node name.
 */
@Singleton
public class NodeNameRegistry {
    private final Map<String, String> nodeNameToNodeUrl = new ConcurrentHashMap<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private volatile ScheduledExecutorService executor;

    /**
     * Start pulling updates from a node.
     *
     * @param nodeUrl Node URL.
     */
    public void startPullingUpdates(String nodeUrl) {
        stopPullingUpdates();
        synchronized (this) {
            if (executor == null) {
                executor = Executors.newScheduledThreadPool(1);
                executor.scheduleWithFixedDelay(() -> updateNodeNames(nodeUrl), 0, 30, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Stops pulling updates.
     */
    public void stopPullingUpdates() {
        synchronized (this) {
            if (executor != null) {
                executor.shutdown();
                executor = null;
            }
        }
    }

    /**
     * Gets a node url by a provided node name.
     */
    public String getNodeUrl(String nodeName) {
        readWriteLock.readLock().lock();
        try {
            return nodeNameToNodeUrl.get(nodeName);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Gets all node names.
     */
    public List<String> getAllNames() {
        readWriteLock.readLock().lock();
        try {
            return new ArrayList<>(nodeNameToNodeUrl.keySet());
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private void updateNodeNames(String nodeUrl) {
        readWriteLock.writeLock().lock();
        try {
            nodeNameToNodeUrl.clear();
            PhysicalTopologyCall topologyCall = new PhysicalTopologyCall();
            topologyCall.execute(new UrlCallInput(nodeUrl)).body()
                    .forEach(it -> {
                        nodeNameToNodeUrl.put(it.getName(), urlFromClusterNode(it));
                    });
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private static String urlFromClusterNode(ClusterNode node) {
        Objects.requireNonNull(node, "node must not be null");
        return "http://" + node.getAddress().getHost() + ":" + node.getMetadata().getRestPort();
    }
}
