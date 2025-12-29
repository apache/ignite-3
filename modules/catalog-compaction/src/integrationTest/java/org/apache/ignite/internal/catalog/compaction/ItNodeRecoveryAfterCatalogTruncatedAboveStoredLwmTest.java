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

package org.apache.ignite.internal.catalog.compaction;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.nio.file.Path;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.configuration.IgnitePaths;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.junit.jupiter.api.Test;

class ItNodeRecoveryAfterCatalogTruncatedAboveStoredLwmTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";
    private static final String TABLE_NAME = "TEST_TABLE";

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return "ignite {\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder.netClusterNodes: [ {} ]\n"
                + "  },\n"
                + "  storage.profiles: {"
                + "        " + DEFAULT_TEST_PROFILE_NAME + ".engine: test, "
                + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
                + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
                + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
                + "  },\n"
                + "  storage.engines: { "
                + "    aipersist: { checkpoint: { "
                + "      intervalMillis: " + 250
                + "    } } "
                + "  },\n"
                + "  clientConnector.port: {},\n"
                + "  rest.port: {},\n"
                + "  failureHandler.dumpThreadsOnFailure: false\n"
                + "}";
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(aggressiveLowWatermarkIncreaseClusterConfig());
    }

    @Test
    void nodeWithLwmBelowEarliestCatalogTsShouldRecover() {
        cluster.aliveNode().sql().executeScript("CREATE ZONE " + ZONE_NAME + " (PARTITIONS 1, REPLICAS 3, AUTO SCALE DOWN 0) "
                + "STORAGE PROFILES ['default']");
        cluster.aliveNode().sql().executeScript("CREATE TABLE " + TABLE_NAME + " (ID INT PRIMARY KEY, VAL VARCHAR) ZONE " + ZONE_NAME);

        int indexOfNodeToStop = 2;
        Path workDirOfStoppedNode = ((IgniteServerImpl) cluster.server(indexOfNodeToStop)).workDir();

        waitTillNodeStoresLwm(indexOfNodeToStop);

        cluster.stopNode(indexOfNodeToStop);

        HybridTimestamp lwmStoredOnStoppedNode = readStoredLwm(workDirOfStoppedNode);

        triggerCatalogCompaction();

        CatalogManager catalogManager = unwrapIgniteImpl(cluster.aliveNode()).catalogManager();
        await().until(() -> catalogManager.earliestCatalog().time() > lwmStoredOnStoppedNode.longValue());

        assertDoesNotThrow(() -> cluster.startNode(indexOfNodeToStop));
    }

    private void waitTillNodeStoresLwm(int indexOfNodeToStop) {
        VaultManager vaultManager = unwrapIgniteImpl(cluster.node(indexOfNodeToStop)).vault();

        await().until(() -> vaultManager.get(LowWatermarkImpl.LOW_WATERMARK_VAULT_KEY), is(notNullValue()));
    }

    private static HybridTimestamp readStoredLwm(Path workDirOfStoppedNode) {
        VaultService vaultService = new PersistentVaultService(IgnitePaths.vaultPath(workDirOfStoppedNode));
        vaultService.start();

        try {
            VaultEntry entry = vaultService.get(LowWatermarkImpl.LOW_WATERMARK_VAULT_KEY);
            assertThat(entry, is(notNullValue()));
            return HybridTimestamp.fromBytes(entry.value());
        } finally {
            vaultService.close();
        }
    }

    private void triggerCatalogCompaction() {
        cluster.aliveNode().sql().executeScript("ALTER TABLE " + TABLE_NAME + " ADD COLUMN ADDED1 INT");
        cluster.aliveNode().sql().executeScript("ALTER TABLE " + TABLE_NAME + " ADD COLUMN ADDED2 INT");
    }
}
