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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class MetaStorageLeaderElectionListenerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "MetaStorageLeaderElectionListenerTest";

    @InjectConfiguration
    private SystemDistributedConfiguration systemConfiguration;

    private ClusterTimeImpl clusterTime;

    @AfterEach
    void cleanup() throws Exception {
        clusterTime.close();
    }

    /**
     * If node lost leadership before Ignite is fully initialized, it could try to stop the safe time scheduler
     * before it was started. Scheduler should not be created in this case, until node regains leadership.
     */
    @Test
    void testSafeTimeSchedulerNotCreatedAfterStoppedTerm() {
        InternalClusterNode thisNode = new ClusterNodeImpl(
                UUID.randomUUID(),
                NODE_NAME,
                new NetworkAddress("host", 1234)
        );

        IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

        clusterTime = spy(new ClusterTimeImpl(NODE_NAME, busyLock, new HybridClockImpl()));

        CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut = new CompletableFuture<>();
        CompletableFuture<SystemDistributedConfiguration> systemConfigurationFuture = completedFuture(systemConfiguration);

        MetaStorageLeaderElectionListener listener = createMetaStorageLeaderElectionListener(
                clusterTime,
                busyLock,
                metaStorageSvcFut,
                systemConfigurationFuture
        );

        long initialTerm = 0;

        listener.onLeaderElected(thisNode, initialTerm);

        // Futures are not completed yet, so safe time scheduler should not be started.
        verifyWithTimeout(clusterTime, 0).startSafeTimeScheduler(any(), any(), eq(initialTerm));

        long lostLeadershipTerm = 1;

        InternalClusterNode otherNode = new ClusterNodeImpl(UUID.randomUUID(), "other", new NetworkAddress("host", 1234));
        listener.onLeaderElected(otherNode, lostLeadershipTerm);

        // This node lost leadership, so safe time scheduler should not be created for previous terms.
        verify(clusterTime, times(1)).stopSafeTimeScheduler(eq(lostLeadershipTerm));

        metaStorageSvcFut.complete(mock(MetaStorageServiceImpl.class));
        systemConfigurationFuture.complete(systemConfiguration);

        verifyWithTimeout(clusterTime, 1).startSafeTimeScheduler(any(), any(), eq(initialTerm));

        long regainedLeadershipTerm = 2;

        listener.onLeaderElected(thisNode, regainedLeadershipTerm);
        verifyWithTimeout(clusterTime, 1).startSafeTimeScheduler(any(), any(), eq(regainedLeadershipTerm));
    }

    private static <T> T verifyWithTimeout(T mock, int times) {
        return verify(mock, timeout(1000).times(times));
    }

    private static MetaStorageLeaderElectionListener createMetaStorageLeaderElectionListener(
            ClusterTimeImpl clusterTime,
            IgniteSpinBusyLock busyLock,
            CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut,
            CompletableFuture<SystemDistributedConfiguration> systemConfigurationFuture
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.nodeName()).thenReturn(NODE_NAME);

        return new MetaStorageLeaderElectionListener(
                busyLock,
                clusterService,
                mock(LogicalTopologyService.class),
                new NoOpFailureManager(),
                metaStorageSvcFut,
                mock(MetaStorageLearnerManager.class),
                clusterTime,
                systemConfigurationFuture,
                List.of(mock(ElectionListener.class)),
                () -> true);
    }
}
