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

package org.apache.ignite.internal.app.di;

import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.internal.app.NodePropertiesImpl;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.compute.ComputeComponentImpl;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.executor.ComputeExecutorImpl;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.metastorage.cache.IdempotentCacheVacuumizer;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.configuration.MetricExtensionConfiguration;
import org.apache.ignite.internal.metrics.sources.ClockServiceMetricSource;
import org.apache.ignite.internal.metrics.sources.JvmMetricSource;
import org.apache.ignite.internal.metrics.sources.OsMetricSource;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.LogicalTopologyEventsListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaSafeTimeTrackerImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory;
import org.apache.ignite.internal.table.distributed.schema.CheckCatalogVersionOnActionRequest;
import org.apache.ignite.internal.table.distributed.schema.CheckCatalogVersionOnAppendEntries;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;

/**
 * Performs post-construction wiring that cannot be expressed as constructor injection.
 * This includes listener registrations, interceptor setups, and cross-component hookups.
 */
@Singleton
public class PostConstructionWiring {
    private final MetricManagerImpl metricManager;
    private final ClockServiceMetricSource clockServiceMetricSource;
    private final MetaStorageManagerImpl metaStorageManager;
    private final CatalogManagerImpl catalogManager;
    private final IdempotentCacheVacuumizer idempotentCacheVacuumizer;
    private final CatalogCompactionRunner catalogCompactionRunner;
    private final SchemaSafeTimeTrackerImpl schemaSafeTimeTracker;
    private final Loza raftManager;
    private final LogicalTopologyImpl logicalTopology;
    private final ClusterService clusterService;
    private final NodeAttributesCollector nodeAttributesCollector;
    private final NodePropertiesImpl nodeProperties;
    private final SystemViewManagerImpl systemViewManager;
    private final TxManagerImpl txManager;
    private final SqlQueryProcessor sqlQueryProcessor;
    private final ComputeComponentImpl computeComponent;
    private final IgniteComputeInternal compute;
    private final KillCommandHandler killCommandHandler;
    private final ComputeExecutorImpl computeExecutor;
    private final ClientHandlerModule clientHandlerModule;
    private final PartitionModificationCounterFactory partitionModificationCounterFactory;
    private final SystemDistributedConfiguration systemDistributedConfiguration;
    private final ConfigurationRegistry clusterConfigRegistry;

    /** Constructor. */
    public PostConstructionWiring(
            MetricManagerImpl metricManager,
            ClockServiceMetricSource clockServiceMetricSource,
            MetaStorageManagerImpl metaStorageManager,
            CatalogManagerImpl catalogManager,
            IdempotentCacheVacuumizer idempotentCacheVacuumizer,
            CatalogCompactionRunner catalogCompactionRunner,
            SchemaSafeTimeTrackerImpl schemaSafeTimeTracker,
            Loza raftManager,
            LogicalTopologyImpl logicalTopology,
            ClusterService clusterService,
            NodeAttributesCollector nodeAttributesCollector,
            NodePropertiesImpl nodeProperties,
            SystemViewManagerImpl systemViewManager,
            TxManagerImpl txManager,
            SqlQueryProcessor sqlQueryProcessor,
            ComputeComponentImpl computeComponent,
            IgniteComputeInternal compute,
            KillCommandHandler killCommandHandler,
            ComputeExecutorImpl computeExecutor,
            ClientHandlerModule clientHandlerModule,
            PartitionModificationCounterFactory partitionModificationCounterFactory,
            SystemDistributedConfiguration systemDistributedConfiguration,
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        this.metricManager = metricManager;
        this.clockServiceMetricSource = clockServiceMetricSource;
        this.metaStorageManager = metaStorageManager;
        this.catalogManager = catalogManager;
        this.idempotentCacheVacuumizer = idempotentCacheVacuumizer;
        this.catalogCompactionRunner = catalogCompactionRunner;
        this.schemaSafeTimeTracker = schemaSafeTimeTracker;
        this.raftManager = raftManager;
        this.logicalTopology = logicalTopology;
        this.clusterService = clusterService;
        this.nodeAttributesCollector = nodeAttributesCollector;
        this.nodeProperties = nodeProperties;
        this.systemViewManager = systemViewManager;
        this.txManager = txManager;
        this.sqlQueryProcessor = sqlQueryProcessor;
        this.computeComponent = computeComponent;
        this.compute = compute;
        this.killCommandHandler = killCommandHandler;
        this.computeExecutor = computeExecutor;
        this.clientHandlerModule = clientHandlerModule;
        this.partitionModificationCounterFactory = partitionModificationCounterFactory;
        this.systemDistributedConfiguration = systemDistributedConfiguration;
        this.clusterConfigRegistry = clusterConfigRegistry;
    }

    /**
     * Performs wiring needed before Phase 1 start.
     * This includes metric source registration and metric configuration.
     */
    public void wirePhase1() {
        JvmMetricSource jvmMetrics = new JvmMetricSource();
        metricManager.registerSource(jvmMetrics);
        metricManager.enable(jvmMetrics);

        OsMetricSource osMetrics = new OsMetricSource();
        metricManager.registerSource(osMetrics);
        metricManager.enable(osMetrics);

        metricManager.registerSource(clockServiceMetricSource);
        metricManager.enable(clockServiceMetricSource);

        partitionModificationCounterFactory.start();
    }

    /**
     * Performs wiring needed before Phase 2 start.
     * This includes MetaStorage listeners, Raft interceptors, system view registrations, and kill handlers.
     */
    public void wirePhase2() {
        // MetaStorage configuration and listeners.
        metaStorageManager.configure(systemDistributedConfiguration);
        metricManager.configure(clusterConfigRegistry.getConfiguration(MetricExtensionConfiguration.KEY).metrics());
        metaStorageManager.addElectionListener(idempotentCacheVacuumizer);
        metaStorageManager.addElectionListener(catalogCompactionRunner::updateCoordinator);
        metaStorageManager.registerNotificationEnqueuedListener(schemaSafeTimeTracker);

        // Raft interceptors.
        raftManager.appendEntriesRequestInterceptor(new CheckCatalogVersionOnAppendEntries(catalogManager));
        raftManager.actionRequestInterceptor(new CheckCatalogVersionOnActionRequest(catalogManager));

        // Node attributes.
        nodeAttributesCollector.register(nodeProperties);
        nodeAttributesCollector.register(systemViewManager);

        // Logical topology listener for joined nodes.
        logicalTopology.addEventListener(logicalTopologyJoinedNodesListener(clusterService.topologyService()));
        logicalTopology.addEventListener(systemViewManager);

        // System view registrations.
        systemViewManager.register(catalogManager);
        systemViewManager.register(txManager);
        systemViewManager.register(sqlQueryProcessor);
        systemViewManager.register(computeComponent);

        // Kill command handlers.
        killCommandHandler.register(transactionKillHandler(txManager));
        killCommandHandler.register(computeKillHandler(compute));

        // Compute <-> client handler bridge.
        computeExecutor.setPlatformComputeTransport(clientHandlerModule);
    }

    private static LogicalTopologyEventListener logicalTopologyJoinedNodesListener(LogicalTopologyEventsListener listener) {
        return new LogicalTopologyEventListener() {
            @Override
            public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                listener.onJoined(joinedNode, newTopology.version());
            }

            @Override
            public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
                listener.onLeft(leftNode, newTopology.version());
            }
        };
    }

    private static OperationKillHandler computeKillHandler(IgniteComputeInternal compute) {
        return new OperationKillHandler() {
            @Override
            public CompletableFuture<Boolean> cancelAsync(String operationId) {
                UUID jobId = UUID.fromString(operationId);
                return compute.cancelAsync(jobId)
                        .thenApply(res -> res != null ? res : Boolean.FALSE);
            }

            @Override
            public boolean local() {
                return false;
            }

            @Override
            public CancellableOperationType type() {
                return CancellableOperationType.COMPUTE;
            }
        };
    }

    private static OperationKillHandler transactionKillHandler(TxManager txManager) {
        return new OperationKillHandler() {
            @Override
            public CompletableFuture<Boolean> cancelAsync(String operationId) {
                UUID transactionId = UUID.fromString(operationId);
                return txManager.kill(transactionId);
            }

            @Override
            public boolean local() {
                return true;
            }

            @Override
            public CancellableOperationType type() {
                return CancellableOperationType.TRANSACTION;
            }
        };
    }
}
