package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.RECOVERY_TRIGGER_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ItHighAvailablePartitionsRecoveryTest  extends ClusterPerTestIntegrationTest {
    private static String ZONE_NAME = "HA_ZONE";

    private static String TABLE_NAME = "TEST_TABLE";

    protected final HybridClock clock = new HybridClockImpl();

    @Override
    protected int initialNodes() {
        return 2;
    }

    @BeforeEach
    void setUp() {
        executeSql(String.format(
                "CREATE ZONE %s WITH REPLICAS=%s, PARTITIONS=%s, STORAGE_PROFILES='%s', CONSISTENCY_MODE='HIGH_AVAILABILITY'",
                ZONE_NAME, initialNodes(), 2, DEFAULT_STORAGE_PROFILE
        ));

        executeSql(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, val INT) ZONE %s",
                TABLE_NAME, ZONE_NAME
        ));
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    void testTopologyReduceEventPropagation() throws InterruptedException {
        IgniteImpl node = igniteImpl(0);

        Supplier<Entry> getRecoveryTriggerKey = () -> {
            CompletableFuture<Entry> getFut = node.metaStorageManager().get(RECOVERY_TRIGGER_KEY);

            assertThat(getFut, willCompleteSuccessfully());

            return getFut.join();
        };

        assertTrue(waitForCondition(() -> getRecoveryTriggerKey.get().empty(), 5_000));

        stopNode(1);

        assertFalse(waitForCondition(() -> getRecoveryTriggerKey.get().empty(), 5_000));

        GroupUpdateRequest request = (GroupUpdateRequest) VersionedSerialization.fromBytes(
                getRecoveryTriggerKey.get().value(), DisasterRecoveryRequestSerializer.INSTANCE);

        int zoneId = node.catalogManager().zone(ZONE_NAME, clock.nowLong()).id();
        int tableId = node.catalogManager().table(TABLE_NAME, clock.nowLong()).id();

        assertEquals(zoneId, request.zoneId());
        assertEquals(tableId, request.tableId());
        assertEquals(Set.of(0,1), request.partitionIds());
        assertFalse(request.manualUpdate());
    }


}
