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
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.ConstantClusterIdSupplier;
import org.apache.ignite.internal.network.MulticastNodeFinder;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(ExecutorServiceExtension.class)
class ItMulticastNodeFinderTest extends IgniteAbstractTest {
    private static final int INIT_PORT = 3344;

    private static final String MULTICAST_GROUP = "224.0.0.1";

    private final ClusterIdSupplier clusterIdSupplier = new ConstantClusterIdSupplier(UUID.randomUUID());

    /** Created {@link ClusterService}s. Needed for resource management. */
    private List<ClusterService> services;

    private TestInfo testInfo;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    void tearDown() {
        assertThat(stopAsync(new ComponentContext(), services), willCompleteSuccessfully());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 255})
    void testFindNodes(int ttl) throws InterruptedException {
        List<NetworkAddress> addresses = findLocalAddresses(INIT_PORT + 1, INIT_PORT + 6);

        services = addresses.stream()
                .map(addr -> startNetwork(testInfo, addr, startMulticastNodeFinder(addr, ttl)))
                .collect(Collectors.toCollection(ArrayList::new)); // ensure mutability

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 5_000), service.topologyService().localMember().toString()
                    + ", topSize=" + service.topologyService().allMembers().size());
        }

        int idx0 = 0;
        int idx1 = 2;

        assertThat(services.get(idx0).stopAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat(services.get(idx1).stopAsync(new ComponentContext()), willCompleteSuccessfully());

        ClusterService svc0 = startNetwork(testInfo, addresses.get(idx0), startMulticastNodeFinder(addresses.get(idx0), ttl));
        services.set(idx0, svc0);

        ClusterService svc2 = startNetwork(testInfo, addresses.get(idx1), startMulticastNodeFinder(addresses.get(idx1), ttl));
        services.set(idx1, svc2);

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 10_000), service.topologyService().localMember().toString()
                    + ", topSize=" + service.topologyService().allMembers().size());
        }
    }

    private static NodeFinder startMulticastNodeFinder(NetworkAddress addr, int ttl) {
        MulticastNodeFinder finder = new MulticastNodeFinder(
                MULTICAST_GROUP,
                INIT_PORT,
                500,
                ttl,
                new InetSocketAddress(addr.host(), addr.port())
        );

        finder.start();

        return finder;
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
        ClusterService clusterService = clusterService(
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
