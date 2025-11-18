package org.apache.ignite.client.fakes;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.deriveUuidFrom;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;

@SuppressWarnings({"ReturnOfNull", "DataFlowIssue"})
public class FakeClusterService implements ClusterService {
    private final String nodeName;

    private final TopologyService topologyService;

    public FakeClusterService(String nodeName) {
        this.nodeName = nodeName;
        this.topologyService = new FakeTopologyService(
                new ClusterNodeImpl(deriveUuidFrom(nodeName), nodeName, new NetworkAddress("127.0.0.1", 8080)));
    }

    @Override
    public String nodeName() {
        return nodeName;
    }

    @Override
    public TopologyService topologyService() {
        return topologyService;
    }

    @Override
    public MessagingService messagingService() {
        return null;
    }

    @Override
    public MessageSerializationRegistry serializationRegistry() {
        return null;
    }

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public void updateMetadata(NodeMetadata metadata) {

    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return CompletableFuture.completedFuture(null);
    }
}
