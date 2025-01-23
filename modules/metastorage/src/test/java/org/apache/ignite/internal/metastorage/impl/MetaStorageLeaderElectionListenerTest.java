package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

public class MetaStorageLeaderElectionListenerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "foo";

    @Test
    void testSafeTimeSchedulerNotCreatedAfterStoppedTerm() {
        ClusterNode thisNode = new ClusterNodeImpl(
                UUID.randomUUID(),
                NODE_NAME,
                new NetworkAddress("host", 1234)
        );

        IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

        ClusterTimeImpl clusterTime = new ClusterTimeImpl(NODE_NAME, busyLock, new HybridClockImpl());

        CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut = new CompletableFuture<>();
        CompletableFuture<MetaStorageConfiguration> metaStorageConfigurationFuture = completedFuture(mock(MetaStorageConfiguration.class));

        MetaStorageLeaderElectionListener listener = createMetaStorageLeaderElectionListener(
                clusterTime,
                busyLock,
                metaStorageSvcFut,
                metaStorageConfigurationFuture
        );

        listener.onLeaderElected(thisNode,0);

        ClusterNode otherNode = new ClusterNodeImpl(UUID.randomUUID(), "other", new NetworkAddress("host", 1234));
        listener.onLeaderElected(otherNode, 1);

        metaStorageSvcFut.complete(mock(MetaStorageServiceImpl.class));
        metaStorageConfigurationFuture.complete(mock(MetaStorageConfiguration.class));

        listener.onLeaderElected(thisNode, 2);
    }

    private static MetaStorageLeaderElectionListener createMetaStorageLeaderElectionListener(
            ClusterTimeImpl clusterTime,
            IgniteSpinBusyLock busyLock,
            CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut,
            CompletableFuture<MetaStorageConfiguration> metaStorageConfigurationFuture
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.nodeName()).thenReturn(NODE_NAME);

        return new MetaStorageLeaderElectionListener(
                busyLock,
                clusterService,
                mock(LogicalTopologyService.class),
                metaStorageSvcFut,
                mock(MetaStorageLearnerManager.class),
                clusterTime,
                metaStorageConfigurationFuture,
                List.of(mock(ElectionListener.class)),
                () -> true);
    }
}
