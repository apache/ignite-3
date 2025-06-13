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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.NodePropertiesImpl;
import org.apache.ignite.internal.configuration.IgnitePaths;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ItColocationStatusHandlingTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 0;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void freshNodeTakesStatusFromSystemProperty(boolean colocationStatusFromSystemProps) {
        runWithColocationProperty(String.valueOf(colocationStatusFromSystemProps), () -> {
            cluster.startAndInit(1);

            assertColocationStatusOnNodeIs(colocationStatusFromSystemProps);
        });
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

    private void assertColocationStatusOnNodeIs(boolean enableColocation) {
        IgniteImpl ignite = unwrappedNode();

        assertThat(ignite.nodeProperties().colocationEnabled(), is(enableColocation));
    }

    private IgniteImpl unwrappedNode() {
        return unwrapIgniteImpl(cluster.node(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void usedNodeTakesStatusFromVaultIfSaved(boolean originalColocationStatus) {
        runWithColocationProperty(String.valueOf(originalColocationStatus), () -> {
            cluster.startAndInit(1);
            cluster.stopNode(0);
        });

        boolean oppositeColocationStatus = !originalColocationStatus;
        runWithColocationProperty(String.valueOf(oppositeColocationStatus), () -> {
            cluster.startNode(0);

            assertColocationStatusOnNodeIs(originalColocationStatus);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void usedNodeWithoutStatusInVaultConsidersColocationDisabled(boolean colocationStatusFromSystemProps) {
        AtomicReference<Path> workDirRef = new AtomicReference<>();

        runWithColocationProperty(String.valueOf(false), () -> {
            cluster.startAndInit(1);

            workDirRef.set(unwrappedNode().workDir());

            cluster.stopNode(0);
        });

        removePersistedColocationStatus(workDirRef.get());

        runWithColocationProperty(String.valueOf(colocationStatusFromSystemProps), () -> {
            cluster.startNode(0);

            assertColocationStatusOnNodeIs(false);
        });
    }

    private static void removePersistedColocationStatus(Path workDir) {
        VaultService vaultService = new PersistentVaultService(IgnitePaths.vaultPath(workDir));

        try {
            vaultService.start();
            vaultService.remove(NodePropertiesImpl.ZONE_BASED_REPLICATION_KEY);
        } finally {
            vaultService.close();
        }
    }
}
