package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.RECOVERY_TRIGGER_KEY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ItHighAvailablePartitionsRecoveryTest  extends ClusterPerTestIntegrationTest {
    private static String ZONE_NAME = "HA_ZONE";

    private static String TABLE_NAME = "TEST_TABLE";

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

    @Test
    void test() throws InterruptedException {
        IgniteImpl node = igniteImpl(0);

        Set<GroupUpdateRequest> receivedRequest = new HashSet<>();

//        assertThat(node.metaStorageManager().get(RECOVERY_TRIGGER_KEY).thenApply(Entry::value), willBe(isNull()));

        node.metaStorageManager().registerExactWatch(RECOVERY_TRIGGER_KEY, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                System.out.println("KKK onupdate");
                assert event.single();

                DisasterRecoveryRequest request = VersionedSerialization.fromBytes(
                        event.entryEvent().newEntry().value(), DisasterRecoveryRequestSerializer.INSTANCE);

                assert request instanceof GroupUpdateRequest;

                receivedRequest.add((GroupUpdateRequest) request);

                return nullCompletedFuture();
            }

            @Override
            public void onError(Throwable e) {
                fail();
            }
        });

        stopNode(1);

        Thread.sleep(20_000);
        System.out.println("KKK " + receivedRequest.size());
        System.out.println("KKK value length " + node.metaStorageManager().get(RECOVERY_TRIGGER_KEY).join().value().length);
        System.out.println("KKK value length " + VersionedSerialization.fromBytes(
                node.metaStorageManager().get(RECOVERY_TRIGGER_KEY).join().value(), DisasterRecoveryRequestSerializer.INSTANCE));
    }
}
