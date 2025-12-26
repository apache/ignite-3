package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.junit.jupiter.api.Test;

public class WriteIntentSwitchHandlerTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(aggressiveLowWatermarkIncreaseClusterConfig());
    }

    private static String aggressiveLowWatermarkIncreaseClusterConfig() {
        return "{\n"
                + "  ignite.gc.lowWatermark {\n"
                + "    dataAvailabilityTimeMillis: 1000,\n"
                + "    updateIntervalMillis: 100\n"
                + "  },\n"
                // Disable tx state storage cleanup.
                + "  ignite.system.properties." + TxManagerImpl.RESOURCE_TTL_PROP + " = " + Long.MAX_VALUE + "\n"
                + "}";
    }

    @Test
    void test() {
        executeSql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        IgniteImpl node1 = unwrapIgniteImpl(cluster.node(1));

        // Failing requests from first to second node.
        failFirst1000SwitchRequests(node0);

        node1.transactions().runInTransaction((tx) -> {
            for (int i = 0; i < 10; i++) {
                executeSql(1, tx, "INSERT INTO test (id, val) VALUES (?, ?)", i, i);
            }
        });

        executeSql("DROP TABLE test");
    }

    private static void failFirst1000SwitchRequests(IgniteImpl node0) {
        AtomicInteger counter = new AtomicInteger(0);

        node0.dropMessages((recipientId, message) -> {
            if (message.getClass().getName().contains("WriteIntentSwitchReplicaRequest")) {
                int i = counter.incrementAndGet();
                return i < 1000;
            }
            return false;
        });
    }
}
