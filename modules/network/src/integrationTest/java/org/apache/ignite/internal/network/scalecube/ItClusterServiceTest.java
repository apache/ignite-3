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

package org.apache.ignite.internal.network.scalecube;

import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for ScaleCube based {@link ClusterService}.
 */
public class ItClusterServiceTest extends BaseIgniteAbstractTest {

    @Test
    void testShutdown(TestInfo testInfo) {
        var addr = new NetworkAddress("localhost", 10000);

        ClusterService service = clusterService(testInfo, addr.port(), new StaticNodeFinder(List.of(addr)));

        service.startAsync();

        service.stopAsync();

        assertThat(service.isStopped(), is(true));

        ExecutionException e = assertThrows(
                ExecutionException.class,
                () -> service.messagingService()
                        .send(mock(ClusterNode.class), mock(NetworkMessage.class))
                        .get(5, TimeUnit.SECONDS)
        );

        assertThat(e.getCause(), isA(NodeStoppingException.class));
    }

    @Test
    void testUpdateMetadata(TestInfo testInfo) throws Exception {
        var addr1 = new NetworkAddress("localhost", 10000);
        var addr2 = new NetworkAddress("localhost", 10001);
        ClusterService service1 = clusterService(testInfo, addr1.port(), new StaticNodeFinder(List.of(addr1, addr2)));
        ClusterService service2 = clusterService(testInfo, addr2.port(), new StaticNodeFinder(List.of(addr1, addr2)));
        service1.startAsync();
        service2.startAsync();
        assertTrue(waitForCondition(() -> service1.topologyService().allMembers().size() == 2, 1000));
        assertTrue(waitForCondition(() -> service2.topologyService().allMembers().size() == 2, 1000));
        try {
            assertThat(service1.topologyService().localMember().nodeMetadata(), is(nullValue()));
            var meta1 = new NodeMetadata("foo", 123, 321);
            var meta2 = new NodeMetadata("bar", 456, 654);
            service1.updateMetadata(meta1);
            service2.updateMetadata(meta2);
            checkLocalMeta(service1, meta1);
            checkLocalMeta(service2, meta2);
            checkRemoteMeta(service1, service2, meta1);
            checkRemoteMeta(service2, service1, meta2);
            checkAllMeta(service1, Set.of(meta1, meta2));
            checkAllMeta(service2, Set.of(meta1, meta2));
        } finally {
            IgniteUtils.closeAll(service1::stopAsync, service2::stopAsync);
        }
    }

    private static void checkLocalMeta(ClusterService service, NodeMetadata expectedMeta) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            ClusterNode localMember = service.topologyService().localMember();
            return expectedMeta.equals(localMember.nodeMetadata());
        }, 1000));
    }

    private static void checkRemoteMeta(
            ClusterService localService, ClusterService remoteService, NodeMetadata expectedMeta
    ) throws InterruptedException {
        ClusterNode localMember = localService.topologyService().localMember();
        assertTrue(waitForCondition(() -> {
            ClusterNode remoteMember = remoteService.topologyService().getByConsistentId(localMember.name());
            return expectedMeta.equals(remoteMember.nodeMetadata());
        }, 1000));
        assertTrue(waitForCondition(() -> {
            ClusterNode remoteMember = remoteService.topologyService().getByAddress(localMember.address());
            return expectedMeta.equals(remoteMember.nodeMetadata());
        }, 1000));
    }

    private static void checkAllMeta(ClusterService service, Set<NodeMetadata> expectedMeta) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            Set<NodeMetadata> actualMeta = service.topologyService().allMembers()
                    .stream()
                    .map(ClusterNode::nodeMetadata)
                    .collect(Collectors.toSet());
            return expectedMeta.equals(actualMeta);
        }, 1000));
    }
}
