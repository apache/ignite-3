package org.apache.ignite.network.scalecube;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITRestartTest {
    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry();

    /** */
    private static final ClusterServiceFactory NETWORK_FACTORY = new ScaleCubeClusterServiceFactory();

    private List<ClusterService> services;

    @AfterEach
    void after() {
        for (ClusterService service : services)
            service.shutdown();
    }

    @Test
    public void testRestart() throws InterruptedException {
        final int initPort = 3344;

        String addr = "localhost";
        List<String> addresses = List.of(addr + ":3344", addr + ":3345", addr + ":3346", addr + ":3347", addr + ":3348");

        services = new ArrayList<>(addresses.size());

        for (int i = 0; i < addresses.size(); i++) {
            String address = addresses.get(i);

            ClusterService svc = startNetwork(address, initPort + i, addresses);

            services.add(svc);
        }

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 5_000), service.topologyService().localMember().toString());
        }

        int idx0 = 0;
        int idx1 = 2;

        services.get(idx0).shutdown();
        services.get(idx1).shutdown();

        ClusterService svc0 = startNetwork(addresses.get(idx0), initPort + idx0, addresses);
        services.set(idx0, svc0);

        ClusterService svc2 = startNetwork(addresses.get(idx1), initPort + idx1, addresses);
        services.set(idx1, svc2);

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 5_000), service.topologyService().localMember().toString());
        }
    }

    /** */
    private ClusterService startNetwork(String name, int port, List<String> addresses) {
        var context = new ClusterLocalConfiguration(name, port, addresses, SERIALIZATION_REGISTRY);

        ClusterService clusterService = NETWORK_FACTORY.createClusterService(context);

        clusterService.start();

        return clusterService;
    }

    /**
     * @param service  The service.
     * @param expected Expected count.
     * @param timeout  The timeout.
     * @return Wait status.
     */
    protected boolean waitForTopology(ClusterService service, int expected, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < stop) {
            if (service.topologyService().allMembers().size() == expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
