package org.apache.ignite.internal.cli;

import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.rest.client.model.ClusterNode;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Singleton
public class NodeNameRegistry {
    private final Map<String, String> nodeNameToNodeUrl = new ConcurrentHashMap<>();
    private volatile ScheduledExecutorService executor;

    public void pullUpdatesFrom(String nodeUrl) {
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
