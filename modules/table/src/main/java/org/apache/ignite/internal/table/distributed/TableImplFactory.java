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

package org.apache.ignite.internal.table.distributed;

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableProperties;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.metrics.ReadWriteMetricSource;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.QualifiedName;

/**
 * Factory for creating {@link TableImpl} instances.
 *
 * <p>Encapsulates all dependencies needed to create {@link InternalTableImpl} and {@link TableImpl}.
 */
public final class TableImplFactory {
    private static final IgniteLogger LOG = Loggers.forClass(TableImplFactory.class);

    private final TopologyService topologyService;

    private final LockManager lockMgr;

    private final ReplicaService replicaSvc;

    private final TxManager txManager;

    private final ClockService clockService;

    private final HybridTimestampTracker observableTimestampTracker;

    private final PlacementDriver executorInclinedPlacementDriver;

    private final TransactionInflights transactionInflights;

    private final SchemaVersions schemaVersions;

    private final ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

    private final Supplier<IgniteSql> sql;

    private final FailureProcessor failureProcessor;

    private final MvTableStorageFactory mvTableStorageFactory;

    private final CatalogService catalogService;

    private final DataStorageManager dataStorageMgr;

    private final MetricManager metricManager;

    private final StreamerFlushExecutorFactory streamerFlushExecutorFactory;

    private volatile StreamerReceiverRunner streamerReceiverRunner;

    /**
     * Creates a new factory.
     *
     * @param topologyService Topology service.
     * @param lockMgr Lock manager.
     * @param replicaSvc Replica service.
     * @param txManager Transaction manager.
     * @param clockService Clock service.
     * @param observableTimestampTracker Observable timestamp tracker.
     * @param executorInclinedPlacementDriver Placement driver.
     * @param transactionInflights Transaction inflights.
     * @param schemaVersions Schema versions.
     * @param sql Supplier for {@link IgniteSql}.
     * @param failureProcessor Failure processor.
     * @param mvTableStorageFactory Factory for creating MV table storages.
     * @param catalogService Catalog service.
     * @param dataStorageMgr Data storage manager.
     * @param metricManager Metric manager.
     * @param streamerFlushExecutorFactory Factory for the streamer flush executor.
     */
    public TableImplFactory(
            TopologyService topologyService,
            LockManager lockMgr,
            ReplicaService replicaSvc,
            TxManager txManager,
            ClockService clockService,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver executorInclinedPlacementDriver,
            TransactionInflights transactionInflights,
            SchemaVersions schemaVersions,
            Supplier<IgniteSql> sql,
            FailureProcessor failureProcessor,
            MvTableStorageFactory mvTableStorageFactory,
            CatalogService catalogService,
            DataStorageManager dataStorageMgr,
            MetricManager metricManager,
            StreamerFlushExecutorFactory streamerFlushExecutorFactory
    ) {
        this.topologyService = topologyService;
        this.lockMgr = lockMgr;
        this.replicaSvc = replicaSvc;
        this.txManager = txManager;
        this.clockService = clockService;
        this.observableTimestampTracker = observableTimestampTracker;
        this.executorInclinedPlacementDriver = executorInclinedPlacementDriver;
        this.transactionInflights = transactionInflights;
        this.schemaVersions = schemaVersions;
        this.sql = sql;
        this.failureProcessor = failureProcessor;
        this.mvTableStorageFactory = mvTableStorageFactory;
        this.catalogService = catalogService;
        this.dataStorageMgr = dataStorageMgr;
        this.metricManager = metricManager;
        this.streamerFlushExecutorFactory = streamerFlushExecutorFactory;
    }

    /**
     * Sets the streamer receiver runner. Must be called before any table creation.
     *
     * @param runner Streamer receiver runner.
     */
    public void setStreamerReceiverRunner(StreamerReceiverRunner runner) {
        this.streamerReceiverRunner = runner;
    }

    /**
     * Creates a {@link TableImpl} wrapping a new {@link InternalTableImpl}.
     *
     * @param tableName Qualified table name.
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog zone descriptor.
     * @param schemaRegistry Schema registry.
     * @return New table instance.
     */
    public TableImpl createTableImpl(
            QualifiedName tableName,
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            SchemaRegistry schemaRegistry
    ) {
        MvTableStorage tableStorage = mvTableStorageFactory.createMvTableStorage(tableDescriptor, zoneDescriptor);

        int partitions = zoneDescriptor.partitions();

        ReadWriteMetricSource metricsSource = createAndRegisterMetricsSource(tableStorage, tableName);

        InternalTableImpl internalTable = new InternalTableImpl(
                tableName,
                zoneDescriptor.id(),
                tableDescriptor.id(),
                partitions,
                topologyService,
                txManager,
                tableStorage,
                replicaSvc,
                clockService,
                observableTimestampTracker,
                executorInclinedPlacementDriver,
                transactionInflights,
                streamerFlushExecutorFactory::get,
                Objects.requireNonNull(streamerReceiverRunner),
                metricsSource
        );

        CatalogTableProperties descProps = tableDescriptor.properties();

        return new TableImpl(
                internalTable,
                lockMgr,
                schemaVersions,
                marshallers,
                sql.get(),
                failureProcessor,
                tableDescriptor.primaryKeyIndexId(),
                new TableStatsStalenessConfiguration(descProps.staleRowsFraction(), descProps.minStaleRowsCount()),
                schemaRegistry
        );
    }

    private ReadWriteMetricSource createAndRegisterMetricsSource(MvTableStorage tableStorage, QualifiedName tableName) {
        StorageTableDescriptor tableDescriptor = tableStorage.getTableDescriptor();

        CatalogTableDescriptor catalogTableDescriptor = catalogService.latestCatalog().table(tableDescriptor.getId());

        // The table might be created during the recovery phase.
        // In that case, we should only register the metric source for the actual tables that exist in the latest catalog.
        boolean registrationNeeded = catalogTableDescriptor != null;

        StorageEngine engine = dataStorageMgr.engineByStorageProfile(tableDescriptor.getStorageProfile());

        // Engine can be null when the storage profile is not available.
        if (engine != null && registrationNeeded) {
            StorageEngineTablesMetricSource engineMetricSource = new StorageEngineTablesMetricSource(engine.name(), tableName);

            engine.addTableMetrics(tableDescriptor, engineMetricSource);

            try {
                metricManager.registerSource(engineMetricSource);
                metricManager.enable(engineMetricSource);
            } catch (Exception e) {
                String message = "Failed to register storage engine metrics source for table [id={}, name={}].";
                LOG.warn(message, e, tableDescriptor.getId(), tableName);
            }
        }

        ReadWriteMetricSource source = new TableMetricSource(tableName);

        if (registrationNeeded) {
            try {
                metricManager.registerSource(source);
                metricManager.enable(source);
            } catch (Exception e) {
                LOG.warn("Failed to register metrics source for table [id={}, name={}].", e, tableDescriptor.getId(), tableName);
            }
        }

        return source;
    }

}
