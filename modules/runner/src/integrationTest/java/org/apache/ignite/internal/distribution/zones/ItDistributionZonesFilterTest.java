package org.apache.ignite.internal.distribution.zones;

import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.NodeWithAttributes;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.Session;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

public class ItDistributionZonesFilterTest extends ClusterPerTestIntegrationTest {
    @Language("JSON")
    private static final String NODE_ATTRIBUTES = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

    private static final Map<String, String> NODE_ATTRIBUTES_MAP = Map.of("region", "US", "storage", "SSD");

    protected static final String COLUMN_KEY = "key";

    protected static final String COLUMN_VAL = "val";

    @Language("JSON")
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_NODE_ATTRIBUTES = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },"
            + "  nodeAttributes: {\n"
            + "    nodeAttributes: " + NODE_ATTRIBUTES
            + "  },\n"
            + "  cluster.failoverTimeout: 100\n"
            + "}";

    @Language("JSON")
    private static String createStartConfig(@Language("JSON") String nodeAttributes) {
        return "{\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ {} ]\n"
                + "    }\n"
                + "  },"
                + "  nodeAttributes: {\n"
                + "    nodeAttributes: " + nodeAttributes
                + "  },\n"
                + "  cluster.failoverTimeout: 100\n"
                + "}";
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return createStartConfig(NODE_ATTRIBUTES);
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void test() throws Exception {
        @Language("JSON") String newNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"HDD\"}}";

        IgniteImpl node = startNode(1, createStartConfig(newNodeAttributes));

        Session session = node.sql().createSession();

        session.execute(null, "CREATE ZONE \"TEST_ZONE\" WITH "
                + "\"REPLICAS\" = 1, "
                + "\"PARTITIONS\" = 2, "
                + "\"DATA_NODES_FILTER\" = '$[?(@.storage == \"SSD\" && @.region == \"US\")]', "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = 0, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = 0");

        session.execute(null, "CREATE TABLE " + "table1" + "("
                + COLUMN_KEY + " INT PRIMARY KEY, " + COLUMN_VAL + " VARCHAR) WITH PRIMARY_ZONE='TEST_ZONE'");

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils.getFieldValue(node, IgniteImpl.class, "metaStorageMgr");

        Entry dataNodesEntry = metaStorageManager.get(zoneDataNodesKey(1)).get(5_000, TimeUnit.MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= dataNodesEntry.revision(), 10_000));

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node, IgniteImpl.class, "distributedTblMgr");

        TableImpl table = (TableImpl) tableManager.table("table1");

        //assertTrue(metaStorageManager.get(fromBytes(stablePartAssignmentsKey(new TablePartitionId(table.tableId(), 0)).bytes())));
        assertTrue(waitForCondition(
                () -> {
                    try {
                        return metaStorageManager.get(stablePartAssignmentsKey(new TablePartitionId(table.tableId(), 0))).get().value() != null;
                    } catch (InterruptedException | ExecutionException e) {
                        fail();
                    }
                    return true;
                },
                10_000)
        );
    }
}
