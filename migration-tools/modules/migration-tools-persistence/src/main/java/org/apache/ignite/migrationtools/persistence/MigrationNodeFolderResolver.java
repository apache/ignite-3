/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
