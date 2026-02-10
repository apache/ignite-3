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

import static org.apache.ignite.internal.network.MulticastNodeFinder.MAX_TTL;
import static org.apache.ignite.internal.network.MulticastNodeFinder.UNSPECIFIED_TTL;
import static org.apache.ignite.internal.network.MulticastNodeFinder.isReservedMacOsInterface;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
import org.apache.ignite.internal.util.IgniteUtils;
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
    private static final int MULTICAST_PORT = 20000;
    private static final String MULTICAST_GROUP = "239.192.0.0";

    private final ClusterIdSupplier clusterIdSupplier = new ConstantClusterIdSupplier(UUID.randomUUID());

    /** Created {@link ClusterService}s. Needed for resource management. */
    private final List<ClusterService> services = new ArrayList<>();
    /** Created {@link NodeFinder}s. Needed for resource management. */
    private final List<NodeFinder> finders = new ArrayList<>();

    private TestInfo testInfo;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    void tearDown() throws Exception {
        assertThat(stopAsync(new ComponentContext(), services), willCompleteSuccessfully());
        services.clear();
        IgniteUtils.closeAllManually(finders);
        finders.clear();
    }

    @ParameterizedTest
    @ValueSource(ints = {UNSPECIFIED_TTL, MAX_TTL})
    void testFindNodes(int ttl) throws SocketException {
        int nodeCount = 5;
        List<NetworkAddress> addresses = findLocalAddresses(INIT_PORT, INIT_PORT + nodeCount);

        for (NetworkAddress address : addresses) {
            NodeFinder finder = startMulticastNodeFinder(address, ttl);
            finders.add(finder);
            services.add(startNetwork(address, finder));
        }

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, nodeCount, 5_000), service.topologyService().localMember().toString()
                    + ", topSize=" + service.topologyService().allMembers().size());
        }

        int idx0 = 0;
        int idx2 = 2;

        assertThat(services.get(idx0).stopAsync(new ComponentContext()), willCompleteSuccessfully());
        finders.get(idx0).close();

        assertThat(services.get(idx2).stopAsync(new ComponentContext()), willCompleteSuccessfully());
        finders.get(idx2).close();

        NodeFinder nodeFinder0 = startMulticastNodeFinder(addresses.get(idx0), ttl);
        ClusterService svc0 = startNetwork(addresses.get(idx0), nodeFinder0);
        finders.set(idx0, nodeFinder0);
        services.set(idx0, svc0);

        NodeFinder nodeFinder2 = startMulticastNodeFinder(addresses.get(idx2), ttl);
        ClusterService svc2 = startNetwork(addresses.get(idx2), nodeFinder2);
        finders.set(idx2, nodeFinder2);
        services.set(idx2, svc2);

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 10_000), service.topologyService().localMember().toString()
                    + ", topSize=" + service.topologyService().allMembers().size());
        }
    }

    private static NodeFinder startMulticastNodeFinder(NetworkAddress addr, int ttl) throws SocketException {
        Set<NetworkAddress> addressesToAdvertise = NetworkInterface.networkInterfaces()
                .filter(itf -> !isReservedMacOsInterface(itf))
                .flatMap(NetworkInterface::inetAddresses)
                .map(address -> new NetworkAddress(address.getHostName(), addr.port()))
                .collect(Collectors.toUnmodifiableSet());

        MulticastNodeFinder finder = new MulticastNodeFinder(
                MULTICAST_GROUP,
                MULTICAST_PORT,
                500,
                ttl,
                "test-node",
                addressesToAdvertise
        );

        finder.start();

        return finder;
    }

    /**
     * Creates a {@link ClusterService} using the given local address and the node finder.
     *
     * @param addr Node address.
     * @param nodeFinder Node finder.
     * @return Created Cluster Service.
     */
    private ClusterService startNetwork(NetworkAddress addr, NodeFinder nodeFinder) {
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
