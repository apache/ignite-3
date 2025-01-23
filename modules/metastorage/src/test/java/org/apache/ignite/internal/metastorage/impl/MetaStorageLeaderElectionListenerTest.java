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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
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
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class MetaStorageLeaderElectionListenerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "foo";

    @InjectConfiguration
    private MetaStorageConfiguration metaStorageConfiguration;

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
        CompletableFuture<MetaStorageConfiguration> metaStorageConfigurationFuture = completedFuture(metaStorageConfiguration);

        MetaStorageLeaderElectionListener listener = createMetaStorageLeaderElectionListener(
                clusterTime,
                busyLock,
                metaStorageSvcFut,
                metaStorageConfigurationFuture
        );

        listener.onLeaderElected(thisNode, 0);

        ClusterNode otherNode = new ClusterNodeImpl(UUID.randomUUID(), "other", new NetworkAddress("host", 1234));
        listener.onLeaderElected(otherNode, 1);

        metaStorageSvcFut.complete(mock(MetaStorageServiceImpl.class));
        metaStorageConfigurationFuture.complete(metaStorageConfiguration);

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
