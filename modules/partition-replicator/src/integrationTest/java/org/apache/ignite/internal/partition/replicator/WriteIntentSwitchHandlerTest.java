package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.awaitility.Awaitility;
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
        String tableName = "test";

        executeSql("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val INT)");

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        IgniteImpl node1 = unwrapIgniteImpl(cluster.node(1));

        CompletableFuture<Void> tableDroppedFut = new CompletableFuture<>();

        failtIntentSwitchUntilTableIsDestroyed(node0, tableDroppedFut);

        node1.transactions().runInTransaction((tx) -> {
            for (int i = 0; i < 10; i++) {
                executeSql(1, tx, "INSERT INTO " + tableName + " (id, val) VALUES (?, ?)", i, i);
            }
        });

        assertThat(node1.distributedTableManager().cachedTable(tableName), notNullValue());

        executeSql("DROP TABLE test");

        // Await real table destruction.
        Awaitility.await().until(() -> node1.distributedTableManager().cachedTable(tableName) == null);
        tableDroppedFut.complete(null);
    }

    private static void failtIntentSwitchUntilTableIsDestroyed(IgniteImpl node0, CompletableFuture<Void> tableDroppedFut) {
        node0.dropMessages((recipientId, message) ->
                message.getClass().getName().contains("WriteIntentSwitchReplicaRequest") && !tableDroppedFut.isDone());
    }
}
