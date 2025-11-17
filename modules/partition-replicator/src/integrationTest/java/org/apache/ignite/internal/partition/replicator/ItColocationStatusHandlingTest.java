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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import java.nio.file.Path;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.NodePropertiesImpl;
import org.apache.ignite.internal.configuration.IgnitePaths;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

class ItColocationStatusHandlingTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 0;
    }

    @Test
    void freshNodeStartWithColocationDisabledFailsWithUnableToStart() {
        runWithColocationProperty("false", () -> {
            assertThrowsWithCause(
                    () -> cluster.startAndInit(1),
                    IgniteException.class,
                    "Table based replication is no longer supported, consider restarting the node in zone based replication mode.");
        });
    }

    @Test
    void usedNodeStartWithColocationDisabledFailsWithUnableToStart() {
        cluster.startAndInit(1);

        Path workDir = unwrappedNode().workDir();

        cluster.stopNode(0);

        setPersistedColocationDisabledStatus(workDir);

        assertThrowsWithCause(
                () -> cluster.startNode(0),
                IgniteException.class,
                "Table based replication is no longer supported.");
    }

    private static void runWithColocationProperty(String propertyValue, Runnable action) {
        String oldValue = System.getProperty(COLOCATION_FEATURE_FLAG);
        System.setProperty(COLOCATION_FEATURE_FLAG, propertyValue);

        try {
            action.run();
        } finally {
            if (oldValue == null) {
                System.clearProperty(COLOCATION_FEATURE_FLAG);
            } else {
                System.setProperty(COLOCATION_FEATURE_FLAG, oldValue);
            }
        }
    }

    private IgniteImpl unwrappedNode() {
        return unwrapIgniteImpl(cluster.node(0));
    }

    private static void setPersistedColocationDisabledStatus(Path workDir) {
        VaultService vaultService = new PersistentVaultService(IgnitePaths.vaultPath(workDir));

        try {
            vaultService.start();
            vaultService.put(NodePropertiesImpl.ZONE_BASED_REPLICATION_KEY, new byte[]{(byte) 0});
        } finally {
            vaultService.close();
        }
    }
}
