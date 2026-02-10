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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.maintenance.MaintenanceProcessor;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.indexing.GridIndexingManager;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneNoopCommunicationSpi;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneNoopDiscoverySpi;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.platform.PlatformNoopProcessor;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.port.GridPortProcessor;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.processors.security.NoOpIgniteSecurityProcessor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.migrationtools.persistence.marshallers.ForeignJdkMarshaller;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.spi.indexing.noop.NoopIndexingSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;

/**
 * Minimal kernal context used to read an Apache Ignite 2 cluster working dir.
 *
 * <p>Inspired in the {@link StandaloneGridKernalContext}.
 */
public class MigrationKernalContext extends GridKernalContextImpl {

    private static final Field CFG_FIELD;

    private static final Field GRID_FIELD;

    private static final Field MARSH_CTX_FIELD;

    private static final Field JDK_MARSHALLER_FIELD;

    static {
        try {
            CFG_FIELD = GridKernalContextImpl.class.getDeclaredField("cfg");
            CFG_FIELD.setAccessible(true);

            GRID_FIELD = GridKernalContextImpl.class.getDeclaredField("grid");
            GRID_FIELD.setAccessible(true);

            MARSH_CTX_FIELD = GridKernalContextImpl.class.getDeclaredField("marshCtx");
            MARSH_CTX_FIELD.setAccessible(true);

            JDK_MARSHALLER_FIELD = MarshallerContextImpl.class.getDeclaredField("jdkMarsh");
            JDK_MARSHALLER_FIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isStarted;

    /**
     * Constructor.
     *
     * @param cfg Ignite 2 cluster configuration.
     * @param nodeFolder Folder of the node.
     * @param nodeConsistentId Consistent id of the node.
     * @throws IgniteCheckedException in case of error.
     */
    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public MigrationKernalContext(IgniteConfiguration cfg, File nodeFolder, Serializable nodeConsistentId) throws IgniteCheckedException {
        var errors = validateConfigurations(cfg);
        if (!errors.isEmpty()) {
            throw new IgniteCheckedException("Error validating ignite configuration: " + errors);
        }

        this.isStarted = false;

        // Disable MBEANS
        IgniteUtils.IGNITE_MBEANS_DISABLED = true;

        MarshallerContextImpl marshCtx = new MarshallerContextImpl(null, null);
        BinaryMarshaller binaryMarsh = new BinaryMarshaller();
        binaryMarsh.setContext(marshCtx);

        IgniteConfiguration adaptedConfiguration = adaptConfiguration(cfg, nodeFolder, nodeConsistentId, binaryMarsh);

        try {
            CFG_FIELD.set(this, adaptedConfiguration);
            JDK_MARSHALLER_FIELD.set(marshCtx, new ForeignJdkMarshaller());
            MARSH_CTX_FIELD.set(this, marshCtx);

            // Unnecessarily required by CacheObjectBinaryProcessorImpl & by GridCacheDefaultAffinityKeyMapper#ignite
            // Also unnecessarily required to be a subclass of IgniteKernel
            GRID_FIELD.set(this, MigrationKernal.create(this, adaptedConfiguration));
        } catch (IllegalAccessException e) {
            throw new IgniteCheckedException(e);
        }

        marshCtx.onMarshallerProcessorStarted(this, null);

        // Set empty plugin processor; Similar to StandaloneIgnitePluginProcessor
        this.add(new IgnitePluginProcessor(this, config(), Collections.emptyList()));

        // Required For GridClusterStateProcessor
        this.add(new GridInternalSubscriptionProcessor(this), false);

        // Direct dependencies of GridCacheProcessor / MigrationCacheProcessor
        this.add(new GridSystemViewManager(this), false);
        this.add(new GridPortProcessor(this), false);
        this.add(new GridClusterStateProcessor(this), true);
        this.add(new GridMetricManager(this), false);
        this.add(new GridTimeoutProcessor(this), false);
        this.add(new GridDeploymentManager(this), false);
        this.add(new GridContinuousProcessor(this), false);
        this.add(new PlatformNoopProcessor(this), false);

        // Add the migration cache processor
        this.add(new MigrationCacheProcessor(this));

        // Direct dependency of MigrationCacheProcessor#startCache
        this.add(new IndexProcessor(this), false);
        this.add(new GridQueryProcessor(this), true);

        this.add(new GridEventStorageManager(this), false);
        this.add(new NoOpIgniteSecurityProcessor(this), false);
        this.add(new PoolProcessor(this), true);
        this.add(new GridIoManager(this), false);

        this.add(new MaintenanceProcessor(this), false);
        this.add(new GridEncryptionManager(this), false);

        this.add(new MigrationNodeFolderResolver(nodeFolder, nodeConsistentId));

        this.add(new GridDiscoveryManager(this), false);
        this.add(new GridResourceProcessor(this), false);

        this.add(new GridAffinityProcessor(this), false);

        this.add(new CompressionProcessor(this));

        this.add(new GridIndexingManager(this), false);

        this.add(new CacheObjectBinaryProcessorImpl(this));
    }

    private static IgniteConfiguration adaptConfiguration(
            IgniteConfiguration cfg,
            File nodeFolder,
            Serializable nodeConsistentId,
            BinaryMarshaller marshaller) {

        var ret = new IgniteConfiguration(cfg);

        var workDir = nodeFolder.getParentFile().getParentFile();
        ret.setWorkDirectory(workDir.getAbsolutePath());
        ret.setConsistentId(nodeConsistentId);

        // Just to make sure these are clean
        ret.setNodeId(UUID.randomUUID());
        ret.setIgniteInstanceName(null);

        // Always disable authentication
        ret.setAuthenticationEnabled(false);
        // Disable clientMode
        ret.setClientMode(false);
        // Should not be necessary
        ret.setPeerClassLoadingEnabled(false);
        // Ensure there's a timeout in the checkpoint configs
        if (ret.getDataStorageConfiguration().getCheckpointReadLockTimeout() == null) {
            ret.getDataStorageConfiguration().setCheckpointReadLockTimeout(10_000);
        }

        CacheConfiguration[] cacheCfgs = ret.getCacheConfiguration();
        if (cacheCfgs == null) {
            ret.setCacheConfiguration();
        } else {
            // TODO: Add test
            // Make sure there are no null cache configurations.
            // This just happens in the migration tools context because we allow skipping errors while loading the beans.
            for (int i = 0; i < cacheCfgs.length; i++) {
                if (cacheCfgs[i] == null) {
                    cacheCfgs[i] = new CacheConfiguration<>();
                }
            }
        }

        ret.setGridLogger(new LoggerBridge());

        // Disable spis
        ret.setDiscoverySpi(new StandaloneNoopDiscoverySpi() {
            @Override
            public Collection<ClusterNode> getRemoteNodes() {
                // This was necessary to skip org.apache.ignite.internal.processors.cache.CacheConfigurationSplitterImpl.supports
                return Collections.emptyList();
            }
        });
        ret.setCommunicationSpi(new StandaloneNoopCommunicationSpi());
        ret.setEventStorageSpi(new NoopEventStorageSpi());
        ret.setMetricExporterSpi(new NoopMetricExporterSpi());
        ret.setSystemViewExporterSpi();
        ret.setDeploymentSpi(new LocalDeploymentSpi());
        ret.setIndexingSpi(new NoopIndexingSpi());

        // Eventually we will need to support encryption
        if (ret.getEncryptionSpi() == null) {
            ret.setEncryptionSpi(new NoopEncryptionSpi());
        }

        ret.setCacheStoreSessionListenerFactories(null);

        ret.setMarshaller(marshaller);

        return ret;
    }

    /** Validates the provided cache configuration. */
    public static List<String> validateConfigurations(IgniteConfiguration cfg) {
        List<String> errors = new ArrayList<>(1);
        if (cfg.getDataStorageConfiguration() == null) {
            errors.add("DataStorageConfiguration must not be null");
        }

        return errors.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(errors);
    }

    /**
     * Starts the context.
     *
     * @throws IgniteCheckedException in case of error.
     */
    public void start() throws IgniteCheckedException {
        // Very simple start tracking mechanism. May be easily corrupted by an exception in the starting procedure.
        if (this.isStarted) {
            return;
        }

        this.isStarted = true;

        // We are not triggering the onKernalStart event. Does not seem to be necessary for now.
        for (GridComponent component : components()) {
            component.start();
        }
    }

    /**
     * Stops the context.
     *
     * @throws IgniteCheckedException in case of error.
     */
    public void stop() throws IgniteCheckedException {
        if (!this.isStarted) {
            return;
        }

        this.isStarted = false;

        List<GridComponent> componentList = components();

        for (ListIterator<GridComponent> it = componentList.listIterator(componentList.size()); it.hasPrevious(); ) {
            GridComponent component = it.previous();
            component.stop(true);
        }
    }
}
