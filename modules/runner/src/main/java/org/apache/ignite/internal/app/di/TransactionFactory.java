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

import io.micronaut.context.annotation.Factory;
import io.micronaut.core.annotation.Order;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.index.IndexNodeFinishedRwTransactionsChecker;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.wrapper.JumpToExecutorByConsistentIdAfterSend;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfiguration;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.configuration.TransactionExtensionConfiguration;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;

/**
 * Micronaut factory for transaction-related components.
 */
@Factory
public class TransactionFactory {
    /** Creates the transaction configuration from the cluster config registry. */
    @Singleton
    public TransactionConfiguration transactionConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry.getConfiguration(TransactionExtensionConfiguration.KEY).transaction();
    }

    /** Creates the system distributed configuration from the cluster config registry. */
    @Singleton
    public SystemDistributedConfiguration systemDistributedConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry.getConfiguration(SystemDistributedExtensionConfiguration.KEY).system();
    }

    /** Creates the GC configuration from the cluster config registry. */
    @Singleton
    public GcConfiguration gcConfiguration(@Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry) {
        return clusterConfigRegistry.getConfiguration(GcExtensionConfiguration.KEY).gc();
    }

    /** Creates the low watermark configuration from the GC config. */
    @Singleton
    public LowWatermarkConfiguration lowWatermarkConfiguration(GcConfiguration gcConfiguration) {
        return gcConfiguration.lowWatermark();
    }

    /** Creates the messaging service wrapper that jumps to the storage operations thread pool. */
    @Singleton
    @Named("storageOperations")
    public MessagingService storageOperationsMessagingService(
            @Named("clusterMessaging") MessagingService clusterMessagingService,
            NodeIdentity nodeIdentity,
            @Named("partitionOperationsExecutor") ExecutorService partitionOperationsExecutor
    ) {
        return new JumpToExecutorByConsistentIdAfterSend(
                clusterMessagingService,
                nodeIdentity.nodeName(),
                message -> partitionOperationsExecutor
        );
    }

    /** Creates the transaction ID generator. */
    @Singleton
    public TransactionIdGenerator transactionIdGenerator(ClusterService clusterService) {
        return new TransactionIdGenerator(() -> clusterService.nodeName().hashCode());
    }

    /** Creates the transaction manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1700)
    public TxManagerImpl txManager(
            NodeIdentity nodeIdentity,
            TransactionConfiguration txConfig,
            SystemDistributedConfiguration systemDistributedConfiguration,
            @Named("storageOperations") MessagingService messagingService,
            TopologyService topologyService,
            ReplicaService replicaService,
            LockManager lockManager,
            VolatileTxStateMetaStorage txStateVolatileStorage,
            ClockService clockService,
            TransactionIdGenerator transactionIdGenerator,
            PlacementDriver placementDriver,
            ReplicationConfiguration replicationConfiguration,
            IndexNodeFinishedRwTransactionsChecker indexNodeFinishedRwTransactionsChecker,
            @Named("partitionOperationsExecutor") ExecutorService partitionOperationsExecutor,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            TransactionInflights transactionInflights,
            LowWatermark lowWatermark,
            FailureManager failureManager,
            MetricManager metricManager,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler
    ) {
        return new TxManagerImpl(
                nodeIdentity.nodeName(),
                txConfig,
                systemDistributedConfiguration,
                messagingService,
                topologyService,
                replicaService,
                lockManager,
                txStateVolatileStorage,
                clockService,
                transactionIdGenerator,
                placementDriver,
                () -> replicationConfiguration.idleSafeTimePropagationDurationMillis().value(),
                indexNodeFinishedRwTransactionsChecker,
                partitionOperationsExecutor,
                resourcesRegistry,
                transactionInflights,
                lowWatermark,
                commonScheduler,
                failureManager,
                metricManager
        );
    }

    /** Creates the shared RocksDB transaction state storage. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1800)
    public TxStateRocksDbSharedStorage txStateRocksDbSharedStorage(
            NodeIdentity nodeIdentity,
            @Named("partitions") ComponentWorkingDir partitionsWorkDir,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler,
            @Named("tableIoExecutor") ScheduledExecutorService tableIoExecutor,
            @Named("partitions") LogStorageManager partitionsLogStorageManager,
            FailureManager failureManager
    ) {
        return new TxStateRocksDbSharedStorage(
                nodeIdentity.nodeName(),
                partitionsWorkDir.dbPath().resolve("tx-state"),
                commonScheduler,
                tableIoExecutor,
                partitionsLogStorageManager.logSyncer(),
                failureManager
        );
    }
}
