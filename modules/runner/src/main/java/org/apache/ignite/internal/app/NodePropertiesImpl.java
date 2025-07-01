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

package org.apache.ignite.internal.app;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.NodeAttributesProvider;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;

/**
 * Default implementation of {@link NodeProperties} using {@link VaultManager} for persistence.
 */
public class NodePropertiesImpl implements NodeProperties, IgniteComponent, NodeAttributesProvider {
    private static final IgniteLogger LOG = Loggers.forClass(NodePropertiesImpl.class);

    public static final ByteArray ZONE_BASED_REPLICATION_KEY = ByteArray.fromString("zone.based.replication");

    private final VaultManager vaultManager;

    private boolean started;

    private boolean colocationEnabled;

    public NodePropertiesImpl(VaultManager vaultManager) {
        this.vaultManager = vaultManager;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        detectAndSaveColocationStatusIfNeeded();

        started = true;

        return nullCompletedFuture();
    }

    @SuppressWarnings("deprecation") // We use a deprecated method intentionally.
    private void detectAndSaveColocationStatusIfNeeded() {
        String logComment;

        VaultEntry entry = vaultManager.get(ZONE_BASED_REPLICATION_KEY);
        if (entry != null) {
            colocationEnabled = entry.value()[0] == 1;

            logComment = "from Vault";
        } else {
            boolean freshNode = vaultManager.name() == null;
            if (freshNode) {
                colocationEnabled = IgniteSystemProperties.colocationEnabled();
                logComment = "from system properties on a fresh node";
            } else {
                colocationEnabled = false;
                logComment = "node of an older version was run without zone based replication";
            }

            saveToVault(colocationEnabled);
        }

        LOG.info("Zone based replication: {} ({})", colocationEnabled, logComment);
        if (colocationEnabled != IgniteSystemProperties.colocationEnabled()) {
            LOG.warn(
                    "Zone based replication status configured via system properties ({}) does not match, it is ignored",
                    IgniteSystemProperties.colocationEnabled()
            );
        }
    }

    private void saveToVault(boolean enablementStatus) {
        vaultManager.put(ZONE_BASED_REPLICATION_KEY, new byte[]{(byte) (enablementStatus ? 1 : 0)});
    }

    @Override
    public boolean colocationEnabled() {
        if (!started) {
            throw new IllegalStateException("NodeProperties not started.");
        }

        return colocationEnabled;
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public Map<String, String> nodeAttributes() {
        return Map.of(COLOCATION_FEATURE_FLAG, Boolean.toString(colocationEnabled()));
    }
}
