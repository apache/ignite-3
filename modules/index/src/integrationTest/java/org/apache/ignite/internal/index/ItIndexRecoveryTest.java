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

package org.apache.ignite.internal.index;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.nio.file.Path;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.configuration.IgnitePaths;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ItIndexRecoveryTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";
    private static final String TABLE_NAME = "TEST_TABLE";
    private static final String INDEX_NAME = "INDEX_TABLE";

    @Override
    protected int initialNodes() {
        return 0;
    }

    private static void aggressiveLowWatermarkIncrease(InitParametersBuilder builder) {
        builder.clusterConfiguration("{\n"
                + "  ignite.gc.lowWatermark {\n"
                + "    dataAvailabilityTimeMillis: 1000,\n"
                + "    updateIntervalMillis: 100\n"
                + "  }\n"
                + "}");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nodeHandlesDroppedTablePrimaryIndexBelowLwmOnRecovery(boolean dropVersionIsNotLast) {
        testNodeRecoveryWithDroppedIndexBelowLwm(() -> {}, this::dropTable, dropVersionIsNotLast);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nodeHandlesDroppedTableSecondaryIndexesBelowLwmOnRecovery(boolean dropVersionIsNotLast) {
        testNodeRecoveryWithDroppedIndexBelowLwm(this::createSecondaryIndex, this::dropTable, dropVersionIsNotLast);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nodeHandlesDroppedSecondaryIndexBelowLwmOnRecovery(boolean dropVersionIsNotLast) {
        testNodeRecoveryWithDroppedIndexBelowLwm(this::createSecondaryIndex, this::dropSecondaryIndex, dropVersionIsNotLast);
    }

    private void testNodeRecoveryWithDroppedIndexBelowLwm(Runnable creator, Runnable dropper, boolean createCatalogVersionAfterDrop) {
        cluster.startAndInit(1, ItIndexRecoveryTest::aggressiveLowWatermarkIncrease);
        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));
        Path workDir0 = ((IgniteServerImpl) cluster.server(0)).workDir();

        createZoneAndTable(1);

        creator.run();

        // We don't want the dropped table to be destroyed before restart.
        disallowLwmRaiseUntilRestart(ignite0);

        dropper.run();

        if (createCatalogVersionAfterDrop) {
            executeUpdate("CREATE TABLE ANOTHER_TABLE (ID INT PRIMARY KEY, VAL VARCHAR)");
        }

        HybridTimestamp tsAfterDrop = latestCatalogVersionTs(ignite0);

        cluster.stopNode(0);

        // Simulate a situation when an LWM was raised (and persisted) and the node was stopped immediately, so we were not
        // able to destroy the dropped table and index yet, even though the drop moment is already under LWM.
        raisePersistedLwm(workDir0, tsAfterDrop.tick());

        assertDoesNotThrow(() -> cluster.startNode(0));
    }

    private void createZoneAndTable(int replicas) {
        executeUpdate(
                "CREATE ZONE " + ZONE_NAME + " (REPLICAS " + replicas + ", PARTITIONS 1) STORAGE PROFILES ['"
                        + DEFAULT_AIPERSIST_PROFILE_NAME + "']"
        );
        executeUpdate("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR) ZONE test_zone");
    }

    private void createSecondaryIndex() {
        executeUpdate("CREATE INDEX " + INDEX_NAME + " ON " + TABLE_NAME + "(val)");
    }

    private void executeUpdate(String query) {
        SqlTestUtils.executeUpdate(query, cluster.aliveNode().sql());
    }

    private static void disallowLwmRaiseUntilRestart(IgniteImpl ignite) {
        ignite.transactions().begin(new TransactionOptions().readOnly(true));
    }

    private void dropTable() {
        executeUpdate("DROP TABLE " + TABLE_NAME);
    }

    private void dropSecondaryIndex() {
        executeUpdate("DROP INDEX " + INDEX_NAME);
    }

    private static HybridTimestamp latestCatalogVersionTs(IgniteImpl ignite) {
        Catalog latestCatalog = ignite.catalogManager().catalog(ignite.catalogManager().latestCatalogVersion());
        return HybridTimestamp.hybridTimestamp(latestCatalog.time());
    }

    private static void raisePersistedLwm(Path workDir, HybridTimestamp newLwm) {
        VaultService vaultService = new PersistentVaultService(IgnitePaths.vaultPath(workDir));

        try {
            vaultService.start();
            vaultService.put(LowWatermarkImpl.LOW_WATERMARK_VAULT_KEY, newLwm.toBytes());
        } finally {
            vaultService.close();
        }
    }
}
