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

package org.apache.ignite.migrationtools.persistence;

import java.io.File;
import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

class MigrationNodeFolderResolver implements PdsFoldersResolver, GridComponent {

    private final PdsFolderSettings<GridCacheDatabaseSharedManager.NodeFileLockHolder> folderSettings;

    public MigrationNodeFolderResolver(File nodeFolder, Serializable consistentId) {
        File parentFolder = nodeFolder.getParentFile();
        String nodeFolderName = nodeFolder.getName();
        this.folderSettings = new PdsFolderSettings<>(
                parentFolder,
                nodeFolderName,
                consistentId,
                null,
                true
        );
    }

    @Override
    public PdsFolderSettings<GridCacheDatabaseSharedManager.NodeFileLockHolder> resolveFolders() {
        return this.folderSettings;
    }

    @Override
    public void start() throws IgniteCheckedException {
        // No-op.
    }

    @Override
    public void stop(boolean cancel) throws IgniteCheckedException {
        // Intentionally left blank
    }

    @Override
    public void onKernalStart(boolean active) throws IgniteCheckedException {
        // Intentionally left blank
    }

    @Override
    public void onKernalStop(boolean cancel) {
        // Intentionally left blank
    }

    @Override
    public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        // Intentionally left blank
    }

    @Override
    public void collectGridNodeData(DiscoveryDataBag dataBag) {
        // Intentionally left blank
    }

    @Override
    public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        // Intentionally left blank
    }

    @Override
    public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        // Intentionally left blank
    }

    @Override
    public void printMemoryStats() {
        // Intentionally left blank
    }

    @Override
    public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        return null;
    }

    @Override
    public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node, DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        return null;
    }

    @Override
    public @Nullable DiscoveryDataExchangeType discoveryDataType() {
        return null;
    }

    @Override
    public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        // Intentionally left blank
    }

    @Override
    public @Nullable IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        return null;
    }
}
