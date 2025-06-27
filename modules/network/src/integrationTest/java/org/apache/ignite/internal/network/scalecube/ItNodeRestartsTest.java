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

import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.ConstantClusterIdSupplier;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests if a topology size is correct after some nodes are restarted in quick succession.
 */
class ItNodeRestartsTest {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ItNodeRestartsTest.class);

    private final ClusterIdSupplier clusterIdSupplier = new ConstantClusterIdSupplier(UUID.randomUUID());

    /** Created {@link ClusterService}s. Needed for resource management. */
    private List<ClusterService> services;

    /** Tear down method. */
    @AfterEach
    void tearDown() {
        assertThat(stopAsync(new ComponentContext(), services), willCompleteSuccessfully());
    }

    /**
     * Tests that restarting nodes get discovered in an established topology.
     */
    @Test
    public void testRestarts(TestInfo testInfo) {
        final int initPort = 3344;

        List<NetworkAddress> addresses = findLocalAddresses(initPort, initPort + 5);

        var nodeFinder = new StaticNodeFinder(addresses);

        services = addresses.stream()
                .map(addr -> startNetwork(testInfo, addr, nodeFinder))
                .collect(Collectors.toCollection(ArrayList::new)); // ensure mutability

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 5_000), service.topologyService().localMember().toString()
                    + ", topSize=" + service.topologyService().allMembers().size());
        }

        int idx0 = 0;
        int idx1 = 2;

        LOG.info("Shutdown {}", addresses.get(idx0));
        assertThat(services.get(idx0).stopAsync(new ComponentContext()), willCompleteSuccessfully());

        LOG.info("Shutdown {}", addresses.get(idx1));
        assertThat(services.get(idx1).stopAsync(new ComponentContext()), willCompleteSuccessfully());

        LOG.info("Starting {}", addresses.get(idx0));
        ClusterService svc0 = startNetwork(testInfo, addresses.get(idx0), nodeFinder);
        services.set(idx0, svc0);

        LOG.info("Starting {}", addresses.get(idx1));
        ClusterService svc2 = startNetwork(testInfo, addresses.get(idx1), nodeFinder);
        services.set(idx1, svc2);

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 10_000), service.topologyService().localMember().toString()
                    + ", topSize=" + service.topologyService().allMembers().size());
        }

        LOG.info("Reached stable state");
    }

    /**
     * Creates a {@link ClusterService} using the given local address and the node finder.
     *
     * @param testInfo   Test info.
     * @param addr       Node address.
     * @param nodeFinder Node finder.
     * @return Created Cluster Service.
     */
    private ClusterService startNetwork(TestInfo testInfo, NetworkAddress addr, NodeFinder nodeFinder) {
        ClusterService clusterService = ClusterServiceTestUtils.clusterService(
                testInfo,
                addr.port(),
                nodeFinder,
                new InMemoryStaleIds(),
                clusterIdSupplier
        );

        assertThat(clusterService.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return clusterService;
    }

    /**
     * Blocks until the given topology reaches {@code expected} amount of members.
     *
     * @param service  The service.
     * @param expected Expected count.
     * @param timeout  The timeout.
     * @return Wait status.
     */
    @SuppressWarnings("BusyWait")
    private static boolean waitForTopology(ClusterService service, int expected, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (service.topologyService().allMembers().size() == expected) {
                return true;
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
