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
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.rest.client.model.ClusterNode;

/**
 * Registry to get a node URL by a node name.
 */
@Singleton
public class NodeNameRegistry {
    private final Map<String, String> nodeNameToNodeUrl = new ConcurrentHashMap<>();
    private volatile ScheduledExecutorService executor;

    /**
     * Start pulling updates from a node.
     *
     * @param nodeUrl Node URL.
     */
    public void startPullingUpdates(String nodeUrl) {
        if (executor != null) {
            synchronized (NodeNameRegistry.class) {
                if (executor != null) {
                    executor.shutdown();
                }
                executor = Executors.newScheduledThreadPool(1);
                executor.scheduleWithFixedDelay(() -> updateNodeNames(nodeUrl), 0, 30, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Stops pulling updates.
     */
    public void stopPullingUpdates() {
        executor.shutdown();
        executor = null;
    }

    public String getNodeUrl(String nodeName) {
        return nodeNameToNodeUrl.get(nodeName);
    }

    public List<String> getAllNames() {
        return new ArrayList<>(nodeNameToNodeUrl.keySet());
    }

    private void updateNodeNames(String nodeUrl) {
        PhysicalTopologyCall topologyCall = new PhysicalTopologyCall();
        topologyCall.execute(new UrlCallInput(nodeUrl)).body()
                .forEach(it -> {
                    nodeNameToNodeUrl.put(it.getName(), urlFromClusterNode(it));
                });
    }

    private static String urlFromClusterNode(ClusterNode node) {
        Objects.requireNonNull(node, "node must not be null");
        return node.getAddress().getHost() + ":" + node.getMetadata().getRestPort();
    }
}
