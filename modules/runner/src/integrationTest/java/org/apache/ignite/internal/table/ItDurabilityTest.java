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

package org.apache.ignite.internal.table;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertionsAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.impl.ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ItDurabilityTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(aggressiveTxStateStorageCleanupClusterConfig());
    }

    private static String aggressiveTxStateStorageCleanupClusterConfig() {
        return "{\n"
                + "  ignite.system.properties." + TxManagerImpl.RESOURCE_TTL_PROP + " = " + 1 + "\n"
                + "}";
    }

    @ParameterizedTest
    @ValueSource(strings = {DEFAULT_AIPERSIST_PROFILE_NAME, DEFAULT_ROCKSDB_PROFILE_NAME})
    @WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "100")
    void pendingRowsLossDoesNotCauseDataLoss(String storageProfile) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-27137 - remove the assumption.
        assumeFalse(DEFAULT_ROCKSDB_PROFILE_NAME.equals(storageProfile), "Not implemented for rocksdb engine yet");

        IgniteImpl node = node();
        createZoneAndTableWithOnePartition(node, storageProfile);

        Table table = node.tables().table(TABLE_NAME);
        KeyValueView<Integer, String> keyValueView = table.keyValueView(Integer.class, String.class);

        node.transactions().runInTransaction(tx -> {
            for (int i = 0; i < 15; i++) {
                putValue(keyValueView, i, tx);
            }

            flushMvStorage(table);

            for (int i = 15; i < 30; i++) {
                putValue(keyValueView, i, tx);
            }
        });

        node = unwrapIgniteImpl(cluster.restartNode(0));
        Table restartedTable = node.tables().table(TABLE_NAME);

        waitTillTxStateStorageIsEmpty(restartedTable, node);

        assertAll30KeysAreAvailable(restartedTable);
    }

    private IgniteImpl node() {
        return unwrapIgniteImpl(cluster.node(0));
    }

    private static void createZoneAndTableWithOnePartition(Ignite node, String storageProfile) {
        String zoneName = "TEST_ZONE";
        IgniteSql sql = node.sql();

        String createZoneSql = String.format(
                "create zone %s (partitions 1, replicas 1) storage profiles ['%s']",
                zoneName,
                storageProfile
        );
        String createTableSql = String.format(
                "create table %s (key int primary key, str varchar) zone %s",
                TABLE_NAME,
                zoneName
        );
        sql.executeScript(createZoneSql + ";" + createTableSql);
    }

    private static void putValue(KeyValueView<Integer, String> kv, int key, Transaction tx) {
        kv.put(tx, key, "str" + key);
    }

    private static void flushMvStorage(Table table) {
        MvPartitionStorage partitionStorage = unwrapTableViewInternal(table).internalTable()
                .storage().getMvPartition(0);
        assertNotNull(partitionStorage);

        assertThat(bypassingThreadAssertionsAsync(partitionStorage::flush), willCompleteSuccessfully());
    }

    private static void waitTillTxStateStorageIsEmpty(Table restartedTable, IgniteImpl node) {
        int zoneId = unwrapTableViewInternal(restartedTable).internalTable().zoneId();
        TxStatePartitionStorage txStateStorage = node.partitionReplicaLifecycleManager().txStatePartitionStorage(zoneId, 0);
        assertNotNull(txStateStorage);

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> bypassingThreadAssertions(() -> storageIsEmpty(txStateStorage)));
    }

    private static boolean storageIsEmpty(TxStatePartitionStorage txStateStorage) {
        try (Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = txStateStorage.scan()) {
            return !cursor.hasNext();
        }
    }

    private static void assertAll30KeysAreAvailable(Table restartedTable) {
        Map<Integer, String> res = restartedTable.keyValueView(Integer.class, String.class).getAll(
                null,
                IntStream.range(0, 30).boxed().collect(toList())
        );

        assertEquals(30, res.size());
        assertEquals(30, res.values().stream().filter(Objects::nonNull).count());
    }
}
