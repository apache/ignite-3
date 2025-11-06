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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_SCAN;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT_ALL;
import static org.apache.ignite.internal.partitiondistribution.Assignments.fromBytes;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toReplicationGroupIdMessage;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.table.distributed.replicator.RemoteResourceIds.cursorId;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.emptyCollectionCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.findAny;
import static org.apache.ignite.internal.util.IgniteUtils.findFirst;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.CURSOR_CLOSE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_TOO_OLD_ERR;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.IgniteTriFunction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.FuturesCleanupResult;
import org.apache.ignite.internal.partition.replicator.ReliableCatalogVersions;
import org.apache.ignite.internal.partition.replicator.ReplicaPrimacy;
import org.apache.ignite.internal.partition.replicator.ReplicaPrimacyEngine;
import org.apache.ignite.internal.partition.replicator.ReplicaTableProcessor;
import org.apache.ignite.internal.partition.replicator.ReplicaTxFinishMarker;
import org.apache.ignite.internal.partition.replicator.ReplicationRaftCommandApplicator;
import org.apache.ignite.internal.partition.replicator.TableAwareReplicaRequestPreProcessor;
import org.apache.ignite.internal.partition.replicator.TxRecoveryEngine;
import org.apache.ignite.internal.partition.replicator.handlers.MinimumActiveTxTimeReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxCleanupRecoveryRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxFinishReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxRecoveryMessageHandler;
import org.apache.ignite.internal.partition.replicator.handlers.TxStateCommitPartitionReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.handlers.VacuumTxStateReplicaRequestHandler;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.TimedBinaryRow;
import org.apache.ignite.internal.partition.replicator.network.command.TimedBinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.command.TimedBinaryRowMessageBuilder;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommandV2Builder;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryTupleMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ChangePeersAndLearnersAsyncReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.GetEstimatedSizeRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSwapRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.partition.replicator.network.replication.ScanCloseReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schemacompat.IncompatibleSchemaVersionException;
import org.apache.ignite.internal.partition.replicator.schemacompat.SchemaCompatibilityValidator;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.CommandApplicationResult;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ReplicatorRecoverableExceptions;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.UnsupportedReplicaRequestException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowUpgrader;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleComparator;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.NullBinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.util.StorageUtils;
import org.apache.ignite.internal.table.RowIdGenerator;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.SortedIndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.TableUtils;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.replicator.handlers.BuildIndexReplicaRequestHandler;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.UpdateCommandResult;
import org.apache.ignite.internal.tx.impl.FullyQualifiedResourceId;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.message.TableWriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.message.TxCleanupRecoveryRequest;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.message.VacuumTxStateReplicaRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequestBase;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicatedInfo;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.CursorUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Partition replication listener. */
public class PartitionReplicaListener implements ReplicaListener, ReplicaTableProcessor {
    /**
     * NB: this listener makes writes to the underlying MV partition storage without taking the partition snapshots read lock. This causes
     * the RAFT snapshots transferred to a follower being slightly inconsistent for a limited amount of time.
     *
     * <p>A RAFT snapshot of a partition consists of MV data, TX state data and metadata (which includes RAFT applied index).
     * Here, the 'slight' inconsistency is that MV data might be ahead of the snapshot meta (namely, RAFT applied index) and TX state data.
     *
     * <p>This listener by its nature cannot advance RAFT applied index (as it works out of the RAFT framework). This alone makes
     * the partition 'slightly inconsistent' in the same way as defined above. So, if we solve this inconsistency, we don't need to take the
     * partition snapshots read lock as well.
     *
     * <p>The inconsistency does not cause any real problems because it is further resolved.
     * <ul>
     *     <li>If the follower with a 'slightly' inconsistent partition state becomes a primary replica, this requires it to apply
     *     whole available RAFT log from the leader before actually becoming a primary; this application will remove the inconsistency</li>
     *     <li>If a node with this inconsistency is going to become a primary, and it's already the leader, then the above will not help.
     *     But write intent resolution procedure will close the gap.</li>
     *     <li>2 items above solve the inconsistency for RW transactions</li>
     *     <li>For RO reading from such a 'slightly inconsistent' partition, write intent resolution closes the gap as well.</li>
     * </ul>
     */
    @SuppressWarnings("unused") // We use it as a placeholder of a documentation which can be linked using # and @see.
    private static final Object INTERNAL_DOC_PLACEHOLDER = null;

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionReplicaListener.class);

    /** Factory to create RAFT command messages. */
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    /** Factory for creating replica command messages. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Replication group id. */
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Use ZonePartitionId instead.
    private final PartitionGroupId replicationGroupId;

    private final int tableId;

    // Despite the fact that it's correct to use replicationGroupId as lock key it's better to preserve slightly higher lock granularity.
    private final TablePartitionId tableLockKey;

    /** Primary key index. */
    private final Lazy<TableSchemaAwareIndexStorage> pkIndexStorage;

    /** Secondary indices. */
    private final Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages;

    /** Versioned partition storage. */
    private final MvPartitionStorage mvDataStorage;

    /** Raft client. */
    private final RaftCommandRunner raftCommandRunner;

    /** Tx manager. */
    private final TxManager txManager;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Handler that processes updates writing them to storage. */
    private final StorageUpdateHandler storageUpdateHandler;

    /** Resources registry. */
    private final RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry;

    /** Clock service. */
    private final ClockService clockService;

    /** Safe time. */
    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTime;

    /** Transaction state resolver. */
    private final TransactionStateResolver transactionStateResolver;

    /** Runs async scan tasks for effective tail recursion execution (avoid deep recursive calls). */
    private final Executor scanRequestExecutor;

    private final Supplier<Map<Integer, IndexLocker>> indexesLockers;

    private final ConcurrentMap<UUID, TxCleanupReadyFutureList> txCleanupReadyFutures = new ConcurrentHashMap<>();

    /** Cleanup futures. */
    private final ConcurrentHashMap<RowId, CompletableFuture<?>> rowCleanupMap = new ConcurrentHashMap<>();

    private final SchemaCompatibilityValidator schemaCompatValidator;

    private final SchemaSyncService schemaSyncService;

    private final CatalogService catalogService;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Processor that handles catalog events {@link CatalogEvent#INDEX_BUILDING} and tracks read-write transaction operations for building
     * indexes.
     */
    private final PartitionReplicaBuildIndexProcessor indexBuildingProcessor;

    private final SchemaRegistry schemaRegistry;

    private final LowWatermark lowWatermark;

    private final NodeProperties nodeProperties;

    private static final boolean SKIP_UPDATES = getBoolean(IgniteSystemProperties.IGNITE_SKIP_STORAGE_UPDATE_IN_BENCHMARK);

    private final TableMetricSource metrics;

    private final ReplicaPrimacyEngine replicaPrimacyEngine;
    private final TableAwareReplicaRequestPreProcessor tableAwareReplicaRequestPreProcessor;
    private final ReliableCatalogVersions reliableCatalogVersions;
    private final ReplicationRaftCommandApplicator raftCommandApplicator;
    private final ReplicaTxFinishMarker replicaTxFinishMarker;

    // Replica request handlers.
    private final TxFinishReplicaRequestHandler txFinishReplicaRequestHandler;
    private final TxStateCommitPartitionReplicaRequestHandler txStateCommitPartitionReplicaRequestHandler;
    private final TxRecoveryMessageHandler txRecoveryMessageHandler;
    private final TxCleanupRecoveryRequestHandler txCleanupRecoveryRequestHandler;
    private final MinimumActiveTxTimeReplicaRequestHandler minimumActiveTxTimeReplicaRequestHandler;
    private final VacuumTxStateReplicaRequestHandler vacuumTxStateReplicaRequestHandler;
    private final BuildIndexReplicaRequestHandler buildIndexReplicaRequestHandler;

    /**
     * The constructor.
     *
     * @param mvDataStorage Data storage.
     * @param raftCommandRunner Raft client.
     * @param txManager Transaction manager.
     * @param lockManager Lock manager.
     * @param replicationGroupId Replication group id.
     * @param tableId Table id.
     * @param indexesLockers Index lock helper objects.
     * @param pkIndexStorage Pk index storage.
     * @param secondaryIndexStorages Secondary index storages.
     * @param clockService Clock service.
     * @param safeTime Safe time clock.
     * @param txStatePartitionStorage Transaction state storage.
     * @param transactionStateResolver Transaction state resolver.
     * @param storageUpdateHandler Handler that processes updates writing them to storage.
     * @param localNode Instance of the local node.
     * @param catalogService Catalog service.
     * @param placementDriver Placement driver.
     * @param clusterNodeResolver Node resolver.
     * @param remotelyTriggeredResourceRegistry Resource registry.
     * @param indexMetaStorage Index meta storage.
     * @param metrics Table metric source.
     */
    public PartitionReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftCommandRunner raftCommandRunner,
            TxManager txManager,
            LockManager lockManager,
            Executor scanRequestExecutor,
            PartitionGroupId replicationGroupId,
            int tableId,
            Supplier<Map<Integer, IndexLocker>> indexesLockers,
            Lazy<TableSchemaAwareIndexStorage> pkIndexStorage,
            Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages,
            ClockService clockService,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
            TxStatePartitionStorage txStatePartitionStorage,
            TransactionStateResolver transactionStateResolver,
            StorageUpdateHandler storageUpdateHandler,
            ValidationSchemasSource validationSchemasSource,
            InternalClusterNode localNode,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            LeasePlacementDriver placementDriver,
            ClusterNodeResolver clusterNodeResolver,
            RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            SchemaRegistry schemaRegistry,
            IndexMetaStorage indexMetaStorage,
            LowWatermark lowWatermark,
            FailureProcessor failureProcessor,
            NodeProperties nodeProperties,
            TableMetricSource metrics
    ) {
        this.mvDataStorage = mvDataStorage;
        this.raftCommandRunner = raftCommandRunner;
        this.txManager = txManager;
        this.lockManager = lockManager;
        this.scanRequestExecutor = scanRequestExecutor;
        this.indexesLockers = indexesLockers;
        this.pkIndexStorage = pkIndexStorage;
        this.secondaryIndexStorages = secondaryIndexStorages;
        this.clockService = clockService;
        this.safeTime = safeTime;
        this.transactionStateResolver = transactionStateResolver;
        this.storageUpdateHandler = storageUpdateHandler;
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
        this.remotelyTriggeredResourceRegistry = remotelyTriggeredResourceRegistry;
        this.schemaRegistry = schemaRegistry;
        this.lowWatermark = lowWatermark;
        this.nodeProperties = nodeProperties;
        this.replicationGroupId = replicationGroupId;
        this.tableId = tableId;
        this.tableLockKey = new TablePartitionId(tableId, replicationGroupId.partitionId());
        this.metrics = metrics;

        this.schemaCompatValidator = new SchemaCompatibilityValidator(validationSchemasSource, catalogService, schemaSyncService);

        indexBuildingProcessor = new PartitionReplicaBuildIndexProcessor(busyLock, tableId, indexMetaStorage, catalogService);

        replicaPrimacyEngine = new ReplicaPrimacyEngine(placementDriver, clockService, replicationGroupId, localNode);

        this.tableAwareReplicaRequestPreProcessor = new TableAwareReplicaRequestPreProcessor(
                clockService,
                schemaCompatValidator,
                schemaSyncService,
                nodeProperties
        );

        reliableCatalogVersions = new ReliableCatalogVersions(schemaSyncService, catalogService);
        raftCommandApplicator = new ReplicationRaftCommandApplicator(raftCommandRunner, replicationGroupId);
        replicaTxFinishMarker = new ReplicaTxFinishMarker(txManager);
        TxRecoveryEngine txRecoveryEngine = new TxRecoveryEngine(
                txManager,
                clusterNodeResolver,
                replicationGroupId,
                this::createAbandonedTxRecoveryEnlistment
        );

        txFinishReplicaRequestHandler = new TxFinishReplicaRequestHandler(
                txStatePartitionStorage,
                clockService,
                txManager,
                validationSchemasSource,
                schemaSyncService,
                catalogService,
                raftCommandRunner,
                replicationGroupId
        );

        txStateCommitPartitionReplicaRequestHandler = new TxStateCommitPartitionReplicaRequestHandler(
                txStatePartitionStorage,
                txManager,
                clusterNodeResolver,
                localNode,
                txRecoveryEngine
        );

        txRecoveryMessageHandler = new TxRecoveryMessageHandler(txStatePartitionStorage, replicationGroupId, txRecoveryEngine);

        txCleanupRecoveryRequestHandler = new TxCleanupRecoveryRequestHandler(
                txStatePartitionStorage,
                txManager,
                failureProcessor,
                replicationGroupId
        );

        minimumActiveTxTimeReplicaRequestHandler = new MinimumActiveTxTimeReplicaRequestHandler(
                clockService,
                raftCommandApplicator);

        vacuumTxStateReplicaRequestHandler = new VacuumTxStateReplicaRequestHandler(raftCommandApplicator);

        buildIndexReplicaRequestHandler = new BuildIndexReplicaRequestHandler(
                indexMetaStorage,
                indexBuildingProcessor.tracker(),
                safeTime,
                raftCommandApplicator);
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove.
    private PendingTxPartitionEnlistment createAbandonedTxRecoveryEnlistment(InternalClusterNode node) {
        assert !nodeProperties.colocationEnabled() : "Unexpected method call within colocation enabled.";
        // Enlistment consistency token is not required for the rollback, so it is 0L.
        // This method is not called in a colocation context, thus it's valid to cast replicationGroupId to TablePartitionId.
        return new PendingTxPartitionEnlistment(node.name(), 0L, ((TablePartitionId) replicationGroupId).tableId());
    }

    @Override
    public CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, UUID senderId) {
        return replicaPrimacyEngine.validatePrimacy(request)
                .thenCompose(replicaPrimacy -> processRequestInContext(request, replicaPrimacy, senderId));
    }

    @Override
    public CompletableFuture<ReplicaResult> process(ReplicaRequest request, ReplicaPrimacy replicaPrimacy, UUID senderId) {
        if (request instanceof ReadWriteMultiRowReplicaRequest) {
            var rwMultiRowPkReq = (ReadWriteMultiRowReplicaRequest) request;

            if (rwMultiRowPkReq.requestType() == RW_UPSERT_ALL) {
                long duration = FastTimestamps.coarseCurrentTimeMillis() - rwMultiRowPkReq.startTs();

                if (duration > 0) {
                    LOG.warn(
                            "PVD:: Processing of RW_UPSERT_ALL request with {} rows took {} ms, "
                                    + "which is longer than the threshold of 100 ms. "
                                    + "Consider using batch operations for better performance.",
                            rwMultiRowPkReq.binaryRows().size(),
                            duration
                    );
                }
            }
        }

        return processRequestInContext(request, replicaPrimacy, senderId);
    }

    private CompletableFuture<ReplicaResult> processRequestInContext(
            ReplicaRequest request,
            ReplicaPrimacy replicaPrimacy,
            UUID senderId
    ) {
        return processRequest(request, replicaPrimacy, senderId)
                .thenApply(PartitionReplicaListener::wrapInReplicaResultIfNeeded);
    }

    private static ReplicaResult wrapInReplicaResultIfNeeded(Object res) {
        if (res instanceof ReplicaResult) {
            return (ReplicaResult) res;
        } else {
            return new ReplicaResult(res, null);
        }
    }

    /** Returns Raft-client. */
    @Override
    public RaftCommandRunner raftClient() {
        if (raftCommandRunner instanceof ExecutorInclinedRaftCommandRunner) {
            return ((ExecutorInclinedRaftCommandRunner) raftCommandRunner).decoratedCommandRunner();
        }
        return raftCommandRunner;
    }

    private CompletableFuture<?> processRequest(ReplicaRequest request, ReplicaPrimacy replicaPrimacy, UUID senderId) {
        boolean hasSchemaVersion = request instanceof SchemaVersionAwareReplicaRequest;

        if (hasSchemaVersion) {
            assert ((SchemaVersionAwareReplicaRequest) request).schemaVersion() > 0 : "No schema version passed?";
        }

        if (request instanceof ReadWriteReplicaRequest) {
            var req = (ReadWriteReplicaRequest) request;

            // Saving state is not needed for full transactions.
            if (!req.full()) {
                txManager.updateTxMeta(req.transactionId(), old -> new TxStateMeta(
                        PENDING,
                        req.coordinatorId(),
                        req.commitPartitionId().asReplicationGroupId(),
                        null,
                        old == null ? null : old.tx(),
                        old == null ? null : old.isFinishedDueToTimeout()
                ));
            }
        }

        if (request instanceof TxRecoveryMessage) {
            assert !nodeProperties.colocationEnabled() : "Unexpected method call within colocation enabled.";

            return txRecoveryMessageHandler.handle((TxRecoveryMessage) request, senderId);
        }

        if (request instanceof TxCleanupRecoveryRequest) {
            assert !nodeProperties.colocationEnabled() : "Unexpected method call within colocation enabled.";

            return txCleanupRecoveryRequestHandler.handle((TxCleanupRecoveryRequest) request);
        }

        if (request instanceof GetEstimatedSizeRequest) {
            return processGetEstimatedSizeRequest();
        }

        if (request instanceof ChangePeersAndLearnersAsyncReplicaRequest) {
            return processChangePeersAndLearnersReplicaRequest((ChangePeersAndLearnersAsyncReplicaRequest) request);
        }

        @Nullable HybridTimestamp opTs = tableAwareReplicaRequestPreProcessor.getOperationTimestamp(request);
        @Nullable HybridTimestamp opTsIfDirectRo = (request instanceof ReadOnlyDirectReplicaRequest) ? opTs : null;
        if (nodeProperties.colocationEnabled()) {
            return processOperationRequestWithTxOperationManagementLogic(senderId, request, replicaPrimacy, opTsIfDirectRo);
        } else {
            // Don't need to validate schema.
            if (opTs == null) {
                assert opTsIfDirectRo == null;
                return processOperationRequestWithTxOperationManagementLogic(senderId, request, replicaPrimacy, null);
            } else {
                return tableAwareReplicaRequestPreProcessor.preProcessTableAwareRequest(request, replicaPrimacy, senderId)
                        .thenCompose(ignored ->
                                processOperationRequestWithTxOperationManagementLogic(senderId, request, replicaPrimacy, opTsIfDirectRo));
            }
        }
    }

    private CompletableFuture<Long> processGetEstimatedSizeRequest() {
        return completedFuture(mvDataStorage.estimatedSize());
    }

    private CompletableFuture<Void> processChangePeersAndLearnersReplicaRequest(ChangePeersAndLearnersAsyncReplicaRequest request) {
        TablePartitionId replicaGrpId = (TablePartitionId) request.groupId().asReplicationGroupId();

        RaftGroupService raftClient = raftCommandRunner instanceof RaftGroupService
                ? (RaftGroupService) raftCommandRunner
                : ((RaftGroupService) ((ExecutorInclinedRaftCommandRunner) raftCommandRunner).decoratedCommandRunner());

        return raftClient.refreshAndGetLeaderWithTerm()
                .exceptionally(throwable -> {
                    throwable = unwrapCause(throwable);

                    if (throwable instanceof TimeoutException) {
                        LOG.info(
                                "Node couldn't get the leader within timeout so the changing peers is skipped [grp={}].",
                                replicaGrpId
                        );

                        return LeaderWithTerm.NO_LEADER;
                    }

                    throw new IgniteInternalException(
                            INTERNAL_ERR,
                            "Failed to get a leader for the RAFT replication group [get=" + replicaGrpId + "].",
                            throwable
                    );
                })
                .thenCompose(leaderWithTerm -> {
                    if (leaderWithTerm.isEmpty() || !replicaPrimacyEngine.tokenStillMatchesPrimary(request.enlistmentConsistencyToken())) {
                        return nullCompletedFuture();
                    }

                    PeersAndLearners peersAndLearners = peersConfigurationFromMessage(request);

                    // run update of raft configuration if this node is a leader
                    LOG.debug("Current node={} is the leader of partition raft group={}. "
                                    + "Initiate rebalance process for partition={}, table={}, peersAndLearners={}",
                            leaderWithTerm.leader(),
                            replicaGrpId,
                            replicaGrpId.partitionId(),
                            replicaGrpId.tableId(),
                            peersAndLearners
                    );

                    return raftClient.changePeersAndLearnersAsync(peersAndLearners, leaderWithTerm.term());
                });
    }

    private static PeersAndLearners peersConfigurationFromMessage(ChangePeersAndLearnersAsyncReplicaRequest request) {
        Assignments pendingAssignments = fromBytes(request.pendingAssignments());

        return fromAssignments(pendingAssignments.nodes());
    }

    private static void setDelayedAckProcessor(@Nullable ReplicaResult result, @Nullable BiConsumer<Object, Throwable> proc) {
        if (result != null) {
            result.delayedAckProcessor = proc;
        }
    }

    /**
     * Process operation request.
     *
     * @param senderId Sender id.
     * @param request Request.
     * @param replicaPrimacy Replica primacy information.
     * @param opStartTsIfDirectRo Start timestamp in case of direct RO tx.
     * @return Future.
     */
    private CompletableFuture<?> processOperationRequest(
            UUID senderId,
            ReplicaRequest request,
            ReplicaPrimacy replicaPrimacy,
            @Nullable HybridTimestamp opStartTsIfDirectRo
    ) {
        if (request instanceof ReadWriteSingleRowReplicaRequest) {
            var req = (ReadWriteSingleRowReplicaRequest) request;

            var opId = new OperationId(senderId, req.timestamp().longValue());

            return appendTxCommand(
                    req.transactionId(),
                    opId,
                    req.requestType(),
                    req.full(),
                    () -> processSingleEntryAction(req, replicaPrimacy.leaseStartTime()).whenComplete(
                            (r, e) -> setDelayedAckProcessor(r, req.delayedAckProcessor()))
            );
        } else if (request instanceof ReadWriteSingleRowPkReplicaRequest) {
            var req = (ReadWriteSingleRowPkReplicaRequest) request;

            var opId = new OperationId(senderId, req.timestamp().longValue());

            return appendTxCommand(
                    req.transactionId(),
                    opId,
                    req.requestType(),
                    req.full(),
                    () -> processSingleEntryAction(req, replicaPrimacy.leaseStartTime()).whenComplete(
                            (r, e) -> setDelayedAckProcessor(r, req.delayedAckProcessor()))
            );
        } else if (request instanceof ReadWriteMultiRowReplicaRequest) {
            var req = (ReadWriteMultiRowReplicaRequest) request;

            var opId = new OperationId(senderId, req.timestamp().longValue());

            return appendTxCommand(
                    req.transactionId(),
                    opId,
                    req.requestType(),
                    req.full(),
                    () -> processMultiEntryAction(req, replicaPrimacy.leaseStartTime()).whenComplete(
                            (r, e) -> setDelayedAckProcessor(r, req.delayedAckProcessor()))
            );
        } else if (request instanceof ReadWriteMultiRowPkReplicaRequest) {
            var req = (ReadWriteMultiRowPkReplicaRequest) request;

            var opId = new OperationId(senderId, req.timestamp().longValue());

            return appendTxCommand(
                    req.transactionId(),
                    opId,
                    req.requestType(),
                    req.full(),
                    () -> processMultiEntryAction(req, replicaPrimacy.leaseStartTime()).whenComplete(
                            (r, e) -> setDelayedAckProcessor(r, req.delayedAckProcessor()))
            );
        } else if (request instanceof ReadWriteSwapRowReplicaRequest) {
            var req = (ReadWriteSwapRowReplicaRequest) request;

            var opId = new OperationId(senderId, req.timestamp().longValue());

            return appendTxCommand(
                    req.transactionId(),
                    opId,
                    req.requestType(),
                    req.full(),
                    () -> processTwoEntriesAction(req, replicaPrimacy.leaseStartTime()).whenComplete(
                            (r, e) -> setDelayedAckProcessor(r, req.delayedAckProcessor()))
            );
        } else if (request instanceof ReadWriteScanRetrieveBatchReplicaRequest) {
            var req = (ReadWriteScanRetrieveBatchReplicaRequest) request;

            // Scan's request.full() has a slightly different semantics than the same field in other requests -
            // it identifies an implicit transaction. Please note that request.full() is always false in the following `appendTxCommand`.
            // We treat SCAN as 2pc and only switch to a 1pc mode if all table rows fit in the bucket and the transaction is implicit.
            // See `req.full() && (err != null || rows.size() < req.batchSize())` condition.
            // If they don't fit the bucket, the transaction is treated as 2pc.
            txManager.updateTxMeta(req.transactionId(), old -> new TxStateMeta(
                    PENDING,
                    req.coordinatorId(),
                    req.commitPartitionId().asReplicationGroupId(),
                    null,
                    old == null ? null : old.tx(),
                    old == null ? null : old.isFinishedDueToTimeout()
            ));

            var opId = new OperationId(senderId, req.timestamp().longValue());

            // Implicit RW scan can be committed locally on a last batch or error.
            return appendTxCommand(req.transactionId(), opId, RW_SCAN, false, () -> processScanRetrieveBatchAction(req))
                    .thenCompose(rows -> {
                        if (allElementsAreNull(rows)) {
                            return completedFuture(rows);
                        } else {
                            return validateRwReadAgainstSchemaAfterTakingLocks(req.transactionId())
                                    .thenApply(ignored -> {
                                        metrics.onRead(rows.size(), false);

                                        return rows;
                                    });
                        }
                    })
                    .whenComplete((rows, err) -> {
                        if (req.full() && (err != null || rows.size() < req.batchSize())) {
                            releaseTxLocks(req.transactionId());
                        }
                    });
        } else if (request instanceof ScanCloseReplicaRequest) {
            processScanCloseAction((ScanCloseReplicaRequest) request);

            return nullCompletedFuture();
        } else if (request instanceof TxFinishReplicaRequest) {
            assert !nodeProperties.colocationEnabled() : request;

            return txFinishReplicaRequestHandler.handle((TxFinishReplicaRequest) request);
        } else if (request instanceof WriteIntentSwitchReplicaRequest) {
            return processWriteIntentSwitchAction((WriteIntentSwitchReplicaRequest) request);
        } else if (request instanceof TableWriteIntentSwitchReplicaRequest) {
            return processTableWriteIntentSwitchAction((TableWriteIntentSwitchReplicaRequest) request);
        } else if (request instanceof ReadOnlySingleRowPkReplicaRequest) {
            return processReadOnlySingleEntryAction((ReadOnlySingleRowPkReplicaRequest) request, replicaPrimacy.isPrimary());
        } else if (request instanceof ReadOnlyMultiRowPkReplicaRequest) {
            return processReadOnlyMultiEntryAction((ReadOnlyMultiRowPkReplicaRequest) request, replicaPrimacy.isPrimary());
        } else if (request instanceof ReadOnlyScanRetrieveBatchReplicaRequest) {
            return processReadOnlyScanRetrieveBatchAction((ReadOnlyScanRetrieveBatchReplicaRequest) request, replicaPrimacy.isPrimary());
        } else if (request instanceof ReplicaSafeTimeSyncRequest) {
            return processReplicaSafeTimeSyncRequest(replicaPrimacy.isPrimary());
        } else if (request instanceof BuildIndexReplicaRequest) {
            return buildIndexReplicaRequestHandler.handle((BuildIndexReplicaRequest) request);
        } else if (request instanceof ReadOnlyDirectSingleRowReplicaRequest) {
            return processReadOnlyDirectSingleEntryAction((ReadOnlyDirectSingleRowReplicaRequest) request, opStartTsIfDirectRo);
        } else if (request instanceof ReadOnlyDirectMultiRowReplicaRequest) {
            return processReadOnlyDirectMultiEntryAction((ReadOnlyDirectMultiRowReplicaRequest) request, opStartTsIfDirectRo);
        } else if (request instanceof TxStateCommitPartitionRequest) {
            assert !nodeProperties.colocationEnabled() : request;

            return txStateCommitPartitionReplicaRequestHandler.handle((TxStateCommitPartitionRequest) request);
        } else if (request instanceof VacuumTxStateReplicaRequest) {
            assert !nodeProperties.colocationEnabled() : request;

            return vacuumTxStateReplicaRequestHandler.handle((VacuumTxStateReplicaRequest) request);
        } else if (request instanceof UpdateMinimumActiveTxBeginTimeReplicaRequest) {
            assert !nodeProperties.colocationEnabled() : request;

            return minimumActiveTxTimeReplicaRequestHandler.handle((UpdateMinimumActiveTxBeginTimeReplicaRequest) request);
        }

        // Unknown request.
        throw new UnsupportedReplicaRequestException(request.getClass());
    }

    /**
     * Processes retrieve batch for read only transaction.
     *
     * @param request Read only retrieve batch request.
     * @param isPrimary Whether the given replica is primary.
     * @return Result future.
     */
    private CompletableFuture<List<BinaryRow>> processReadOnlyScanRetrieveBatchAction(
            ReadOnlyScanRetrieveBatchReplicaRequest request,
            boolean isPrimary
    ) {
        UUID txId = request.transactionId();
        int batchCount = request.batchSize();
        HybridTimestamp readTimestamp = request.readTimestamp();

        FullyQualifiedResourceId cursorId = cursorId(txId, request.scanId());

        CompletableFuture<Void> safeReadFuture = isPrimaryInTimestamp(isPrimary, readTimestamp)
                ? nullCompletedFuture()
                : safeTime.waitFor(readTimestamp);

        if (request.indexToUse() != null) {
            TableSchemaAwareIndexStorage indexStorage = secondaryIndexStorages.get().get(request.indexToUse());

            if (indexStorage == null) {
                throw new AssertionError("Index not found: uuid=" + request.indexToUse());
            }

            if (request.exactKey() != null) {
                assert request.lowerBoundPrefix() == null && request.upperBoundPrefix() == null : "Index lookup doesn't allow bounds.";

                return safeReadFuture
                        .thenCompose(unused -> lookupIndex(request, indexStorage))
                        .thenApply(rows -> {
                            metrics.onRead(rows.size(), true);

                            return rows;
                        });
            }

            assert indexStorage.storage() instanceof SortedIndexStorage;

            return safeReadFuture
                    .thenCompose(unused -> scanSortedIndex(request, indexStorage))
                    .thenApply(rows -> {
                        metrics.onRead(rows.size(), true);

                        return rows;
                    });
        }

        return safeReadFuture
                .thenCompose(
                        unused -> retrieveExactEntriesUntilCursorEmpty(txId, request.coordinatorId(), readTimestamp, cursorId, batchCount))
                .thenApply(rows -> {
                    metrics.onRead(rows.size(), true);

                    return rows;
                });
    }

    /**
     * Extracts exact amount of entries, or less if cursor is become empty, from a cursor on the specific time.
     *
     * @param txId Transaction id is used for RW only.
     * @param txCoordinatorId Transaction coordinator id.
     * @param readTimestamp Timestamp of the moment when that moment when the data will be extracted.
     * @param cursorId Cursor id.
     * @param count Amount of entries which sill be extracted.
     * @return Result future.
     */
    private CompletableFuture<List<BinaryRow>> retrieveExactEntriesUntilCursorEmpty(
            UUID txId,
            UUID txCoordinatorId,
            @Nullable HybridTimestamp readTimestamp,
            FullyQualifiedResourceId cursorId,
            int count
    ) {
        var result = new ArrayList<BinaryRow>(count);

        return retrieveExactEntriesUntilCursorEmpty(txId, txCoordinatorId, readTimestamp, cursorId, count, result)
                .thenApply(v -> {
                    closeCursorIfBatchNotFull(result, count, cursorId);

                    return result;
                });
    }

    private CompletableFuture<Void> retrieveExactEntriesUntilCursorEmpty(
            UUID txId,
            UUID txCoordinatorId,
            @Nullable HybridTimestamp readTimestamp,
            FullyQualifiedResourceId cursorId,
            int count,
            List<BinaryRow> result
    ) {
        CursorResource resource = remotelyTriggeredResourceRegistry.register(
                cursorId,
                txCoordinatorId,
                () -> new CursorResource(mvDataStorage.scan(readTimestamp == null ? HybridTimestamp.MAX_VALUE : readTimestamp))
        );

        PartitionTimestampCursor cursor = resource.cursor();

        int resultStartIndex = result.size();

        var resolutionFutures = new ArrayList<CompletableFuture<TimedBinaryRow>>();

        while (result.size() < count && cursor.hasNext()) {
            ReadResult readResult = cursor.next();

            UUID retrievedResultTxId = readResult.transactionId();

            if (!readResult.isWriteIntent() || (readTimestamp == null && txId.equals(retrievedResultTxId))) {
                BinaryRow row = readResult.binaryRow();

                if (row != null) {
                    result.add(row);
                }
            } else {
                HybridTimestamp newestCommitTimestamp = readResult.newestCommitTimestamp();

                TimedBinaryRow candidate;
                if (newestCommitTimestamp == null) {
                    candidate = null;
                } else {
                    // TODO: Calling "cursor.committed" here may lead to performance degradation in presence of many write intents.
                    //  This part should probably moved inside the "() -> candidate" lambda.
                    //  See https://issues.apache.org/jira/browse/IGNITE-26052.
                    BinaryRow committedRow = cursor.committed(newestCommitTimestamp);

                    candidate = committedRow == null ? null : new TimedBinaryRow(committedRow, newestCommitTimestamp);
                }

                resolutionFutures.add(resolveWriteIntentAsync(readResult, readTimestamp, () -> candidate));

                // Add a placeholder in the result array to later transfer the resolved write intent.
                result.add(null);
            }
        }

        if (resolutionFutures.isEmpty()) {
            return nullCompletedFuture();
        }

        return allOf(resolutionFutures.toArray(CompletableFuture[]::new))
                .thenComposeAsync(unused -> {
                    // After waiting for the futures to complete, we need to merge the resolved write intents with the retrieved rows
                    // preserving the original order.
                    mergeRowsWithResolvedWriteIntents(result, resultStartIndex, resolutionFutures);

                    if (result.size() < count && cursor.hasNext()) {
                        return retrieveExactEntriesUntilCursorEmpty(txId, txCoordinatorId, readTimestamp, cursorId, count, result);
                    } else {
                        return nullCompletedFuture();
                    }
                }, scanRequestExecutor);
    }

    /**
     * Extracts exact amount of entries, or less if cursor is become empty, from a cursor on the specific time. Use it for RW.
     *
     * @param txId Transaction id.
     * @param cursorId Cursor id.
     * @return Future finishes with the resolved binary row.
     */
    private CompletableFuture<List<BinaryRow>> retrieveExactEntriesUntilCursorEmpty(
            UUID txId,
            UUID txCoordinatorId,
            FullyQualifiedResourceId cursorId,
            int count
    ) {
        return retrieveExactEntriesUntilCursorEmpty(txId, txCoordinatorId, null, cursorId, count).thenCompose(rows -> {
            if (nullOrEmpty(rows)) {
                return emptyListCompletedFuture();
            }

            CompletableFuture<?>[] futs = new CompletableFuture[rows.size()];

            for (int i = 0; i < rows.size(); i++) {
                BinaryRow row = rows.get(i);

                futs[i] = validateBackwardCompatibility(row, txId);
            }

            return allOf(futs).thenApply((unused) -> rows);
        });
    }

    private static void mergeRowsWithResolvedWriteIntents(
            List<BinaryRow> result,
            int resultStartIndex,
            List<CompletableFuture<TimedBinaryRow>> resolutionFutures
    ) {
        assert !resolutionFutures.isEmpty();

        int futuresIndex = 0;

        ListIterator<BinaryRow> it = result.listIterator(resultStartIndex);

        while (it.hasNext()) {
            BinaryRow row = it.next();

            if (row == null) {
                CompletableFuture<TimedBinaryRow> future = resolutionFutures.get(futuresIndex++);

                assert future.isDone();

                TimedBinaryRow resolvedReadResult = future.join();

                BinaryRow resolvedBinaryRow = resolvedReadResult == null ? null : resolvedReadResult.binaryRow();

                if (resolvedBinaryRow == null) {
                    it.remove();
                } else {
                    it.set(resolvedBinaryRow);
                }
            }
        }

        assert futuresIndex == resolutionFutures.size();
    }

    private CompletableFuture<Void> validateBackwardCompatibility(BinaryRow row, UUID txId) {
        return schemaCompatValidator.validateBackwards(row.schemaVersion(), tableId(), txId)
                .thenAccept(validationResult -> {
                    if (!validationResult.isSuccessful()) {
                        throw new IncompatibleSchemaVersionException(String.format(
                                "Operation failed because it tried to access a row with newer schema version than transaction's [table=%s, "
                                        + "txSchemaVersion=%d, rowSchemaVersion=%d]",
                                validationResult.failedTableName(), validationResult.fromSchemaVersion(), validationResult.toSchemaVersion()
                        ));
                    }
                });
    }

    /**
     * Processes single entry request for read only transaction.
     *
     * @param request Read only single entry request.
     * @param isPrimary Whether the given replica is primary.
     * @return Result future.
     */
    private CompletableFuture<BinaryRow> processReadOnlySingleEntryAction(ReadOnlySingleRowPkReplicaRequest request, boolean isPrimary) {
        BinaryTuple primaryKey = resolvePk(request.primaryKey());
        HybridTimestamp readTimestamp = request.readTimestamp();

        if (request.requestType() != RO_GET) {
            throw new IgniteInternalException(
                    Replicator.REPLICA_COMMON_ERR,
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        CompletableFuture<Void> safeReadFuture = isPrimaryInTimestamp(isPrimary, readTimestamp)
                ? nullCompletedFuture()
                : safeTime.waitFor(request.readTimestamp());

        return safeReadFuture.thenCompose(unused -> resolveRowByPkForReadOnly(primaryKey, readTimestamp));
    }

    /**
     * Checks that the node is primary and {@code timestamp} is already passed in the reference system of the current node.
     *
     * @param isPrimary True if the node is primary, false otherwise.
     * @param timestamp Timestamp to check.
     * @return True if the timestamp is already passed in the reference system of the current node and node is primary, false otherwise.
     */
    private boolean isPrimaryInTimestamp(boolean isPrimary, HybridTimestamp timestamp) {
        return isPrimary && clockService.now().compareTo(timestamp) > 0;
    }

    /**
     * Processes multiple entries request for read only transaction.
     *
     * @param request Read only multiple entries request.
     * @param isPrimary Whether the given replica is primary.
     * @return Result future.
     */
    private CompletableFuture<List<BinaryRow>> processReadOnlyMultiEntryAction(
            ReadOnlyMultiRowPkReplicaRequest request,
            boolean isPrimary
    ) {
        List<BinaryTuple> primaryKeys = resolvePks(request.primaryKeys());
        HybridTimestamp readTimestamp = request.readTimestamp();

        if (request.requestType() != RO_GET_ALL) {
            throw new IgniteInternalException(
                    Replicator.REPLICA_COMMON_ERR,
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        CompletableFuture<Void> safeReadFuture = isPrimaryInTimestamp(isPrimary, readTimestamp)
                ? nullCompletedFuture()
                : safeTime.waitFor(request.readTimestamp());

        return safeReadFuture.thenCompose(unused -> {
            CompletableFuture<BinaryRow>[] resolutionFuts = new CompletableFuture[primaryKeys.size()];

            for (int i = 0; i < primaryKeys.size(); i++) {
                resolutionFuts[i] = resolveRowByPkForReadOnly(primaryKeys.get(i), readTimestamp);
            }

            return allOfToList(resolutionFuts);
        });
    }

    /**
     * Handler to process {@link ReplicaSafeTimeSyncRequest}.
     *
     * @param isPrimary Whether is primary replica.
     * @return Future.
     */
    private CompletableFuture<?> processReplicaSafeTimeSyncRequest(boolean isPrimary) {
        // Disable safe-time sync if the Colocation feature is enabled, safe-time is managed on a different level there.
        if (!isPrimary || nodeProperties.colocationEnabled()) {
            return nullCompletedFuture();
        }

        return applyCmdWithExceptionHandling(
                REPLICA_MESSAGES_FACTORY.safeTimeSyncCommand().initiatorTime(clockService.now()).build()
        );
    }

    /**
     * Processes scan close request.
     *
     * @param request Scan close request operation.
     */
    private void processScanCloseAction(ScanCloseReplicaRequest request) {
        UUID txId = request.transactionId();

        FullyQualifiedResourceId cursorId = cursorId(txId, request.scanId());

        try {
            remotelyTriggeredResourceRegistry.close(cursorId);
        } catch (IgniteException e) {
            throw wrapCursorCloseException(e);
        }
    }

    /**
     * Closes a cursor if the batch is not fully retrieved.
     *
     * @param batchSize Requested batch size.
     * @param rows List of retrieved batch items.
     * @param cursorId Cursor id.
     */
    private void closeCursorIfBatchNotFull(List<?> rows, int batchSize, FullyQualifiedResourceId cursorId) {
        if (rows.size() < batchSize) {
            try {
                remotelyTriggeredResourceRegistry.close(cursorId);
            } catch (IgniteException e) {
                throw wrapCursorCloseException(e);
            }
        }
    }

    private ReplicationException wrapCursorCloseException(IgniteException e) {
        return new ReplicationException(CURSOR_CLOSE_ERR,
                format("Close cursor exception [replicaGrpId={}, msg={}]", replicationGroupId, e.getMessage()), e);
    }

    /**
     * Processes scan retrieve batch request.
     *
     * @param request Scan retrieve batch request operation.
     * @return Listener response.
     */
    private CompletableFuture<List<BinaryRow>> processScanRetrieveBatchAction(ReadWriteScanRetrieveBatchReplicaRequest request) {
        if (request.indexToUse() != null) {
            TableSchemaAwareIndexStorage indexStorage = secondaryIndexStorages.get().get(request.indexToUse());

            if (indexStorage == null) {
                throw new AssertionError("Index not found: uuid=" + request.indexToUse());
            }

            if (request.exactKey() != null) {
                assert request.lowerBoundPrefix() == null && request.upperBoundPrefix() == null : "Index lookup doesn't allow bounds.";

                return lookupIndex(request, indexStorage.storage(), request.coordinatorId());
            }

            assert indexStorage.storage() instanceof SortedIndexStorage;

            return scanSortedIndex(request, indexStorage);
        }

        UUID txId = request.transactionId();
        int batchCount = request.batchSize();

        FullyQualifiedResourceId cursorId = cursorId(txId, request.scanId());

        return lockManager.acquire(txId, new LockKey(tableLockKey), LockMode.S)
                .thenCompose(tblLock -> retrieveExactEntriesUntilCursorEmpty(txId, request.coordinatorId(), cursorId, batchCount));
    }

    /**
     * Lookup sorted index in RO tx.
     *
     * @param request Index scan request.
     * @param schemaAwareIndexStorage Index storage.
     * @return Operation future.
     */
    private CompletableFuture<List<BinaryRow>> lookupIndex(
            ReadOnlyScanRetrieveBatchReplicaRequest request,
            TableSchemaAwareIndexStorage schemaAwareIndexStorage
    ) {
        IndexStorage indexStorage = schemaAwareIndexStorage.storage();

        FullyQualifiedResourceId cursorId = cursorId(request.transactionId(), request.scanId());

        BinaryTuple key = request.exactKey().asBinaryTuple();

        Cursor<RowId> cursor = remotelyTriggeredResourceRegistry.<CursorResource>register(
                cursorId,
                request.coordinatorId(),
                () -> new CursorResource(indexStorage.get(key))
        ).cursor();

        Cursor<IndexRow> indexRowCursor = CursorUtils.map(cursor, rowId -> new IndexRowImpl(key, rowId));

        int batchCount = request.batchSize();

        var result = new ArrayList<BinaryRow>(batchCount);

        HybridTimestamp readTimestamp = request.readTimestamp();

        return continueReadOnlyIndexScan(
                schemaAwareIndexStorage,
                indexRowCursor,
                readTimestamp,
                batchCount,
                result,
                tableVersionByTs(readTimestamp)
        ).thenApply(ignore -> {
            closeCursorIfBatchNotFull(result, batchCount, cursorId);

            return result;
        });
    }

    private CompletableFuture<List<BinaryRow>> lookupIndex(
            ReadWriteScanRetrieveBatchReplicaRequest request,
            IndexStorage indexStorage,
            UUID txCoordinatorId
    ) {
        UUID txId = request.transactionId();
        int batchCount = request.batchSize();

        FullyQualifiedResourceId cursorId = cursorId(txId, request.scanId());

        Integer indexId = request.indexToUse();

        BinaryTuple exactKey = request.exactKey().asBinaryTuple();

        return lockManager.acquire(txId, new LockKey(indexId, exactKey.byteBuffer()), LockMode.S)
                .thenCompose(indRowLock -> { // Hash index bucket S lock
                    Cursor<RowId> cursor = remotelyTriggeredResourceRegistry.<CursorResource>register(cursorId, txCoordinatorId,
                            () -> new CursorResource(indexStorage.get(exactKey))).cursor();

                    var result = new ArrayList<BinaryRow>(batchCount);

                    return continueIndexLookup(txId, cursor, batchCount, result)
                            .thenApply(ignore -> {
                                closeCursorIfBatchNotFull(result, batchCount, cursorId);

                                return result;
                            });
                });
    }

    /**
     * Scans sorted index in RW tx.
     *
     * @param request Index scan request.
     * @param schemaAwareIndexStorage Sorted index storage.
     * @return Operation future.
     */
    private CompletableFuture<List<BinaryRow>> scanSortedIndex(
            ReadWriteScanRetrieveBatchReplicaRequest request,
            TableSchemaAwareIndexStorage schemaAwareIndexStorage
    ) {
        var indexStorage = (SortedIndexStorage) schemaAwareIndexStorage.storage();

        UUID txId = request.transactionId();

        FullyQualifiedResourceId cursorId = cursorId(txId, request.scanId());

        Integer indexId = request.indexToUse();

        BinaryTupleMessage lowerBoundMessage = request.lowerBoundPrefix();
        BinaryTupleMessage upperBoundMessage = request.upperBoundPrefix();

        BinaryTuplePrefix lowerBound = lowerBoundMessage == null ? null : lowerBoundMessage.asBinaryTuplePrefix();
        BinaryTuplePrefix upperBound = upperBoundMessage == null ? null : upperBoundMessage.asBinaryTuplePrefix();

        int flags = request.flags();

        BinaryTupleComparator comparator = StorageUtils.binaryTupleComparator(indexStorage.indexDescriptor().columns());

        Predicate<IndexRow> isUpperBoundAchieved = indexRow -> {
            if (indexRow == null) {
                return true;
            }

            if (upperBound == null) {
                return false;
            }

            ByteBuffer buffer = upperBound.byteBuffer();

            if ((flags & SortedIndexStorage.LESS_OR_EQUAL) != 0) {
                byte boundFlags = buffer.get(0);

                buffer.put(0, (byte) (boundFlags | BinaryTupleCommon.EQUALITY_FLAG));
            }

            return comparator.compare(indexRow.indexColumns().byteBuffer(), buffer) >= 0;
        };

        Cursor<IndexRow> cursor = remotelyTriggeredResourceRegistry.<CursorResource>register(
                cursorId,
                request.coordinatorId(),
                () -> new CursorResource(indexStorage.scan(
                        lowerBound,
                        // We have to handle upperBound on a level of replication listener,
                        // for correctness of taking of a range lock.
                        null,
                        flags
                ))
        ).cursor();

        SortedIndexLocker indexLocker = (SortedIndexLocker) indexesLockers.get().get(indexId);

        int batchCount = request.batchSize();

        var result = new ArrayList<BinaryRow>(batchCount);

        return continueIndexScan(
                txId,
                schemaAwareIndexStorage,
                indexLocker,
                cursor,
                batchCount,
                result,
                isUpperBoundAchieved,
                tableVersionByTs(beginTimestamp(txId))
        ).thenApply(ignore -> {
            closeCursorIfBatchNotFull(result, batchCount, cursorId);

            return result;
        });
    }

    /**
     * Scans sorted index in RO tx.
     *
     * @param request Index scan request.
     * @param schemaAwareIndexStorage Sorted index storage.
     * @return Operation future.
     */
    private CompletableFuture<List<BinaryRow>> scanSortedIndex(
            ReadOnlyScanRetrieveBatchReplicaRequest request,
            TableSchemaAwareIndexStorage schemaAwareIndexStorage
    ) {
        var indexStorage = (SortedIndexStorage) schemaAwareIndexStorage.storage();

        FullyQualifiedResourceId cursorId = cursorId(request.transactionId(), request.scanId());

        BinaryTupleMessage lowerBoundMessage = request.lowerBoundPrefix();
        BinaryTupleMessage upperBoundMessage = request.upperBoundPrefix();

        BinaryTuplePrefix lowerBound = lowerBoundMessage == null ? null : lowerBoundMessage.asBinaryTuplePrefix();
        BinaryTuplePrefix upperBound = upperBoundMessage == null ? null : upperBoundMessage.asBinaryTuplePrefix();

        int flags = request.flags();

        Cursor<IndexRow> cursor = remotelyTriggeredResourceRegistry.<CursorResource>register(cursorId, request.coordinatorId(),
                () -> new CursorResource(indexStorage.readOnlyScan(
                        lowerBound,
                        upperBound,
                        flags
                ))).cursor();

        int batchCount = request.batchSize();

        var result = new ArrayList<BinaryRow>(batchCount);

        HybridTimestamp readTimestamp = request.readTimestamp();

        return continueReadOnlyIndexScan(
                schemaAwareIndexStorage,
                cursor,
                readTimestamp,
                batchCount,
                result,
                tableVersionByTs(readTimestamp)
        ).thenApply(ignore -> {
            closeCursorIfBatchNotFull(result, batchCount, cursorId);

            return result;
        });
    }

    private CompletableFuture<Void> continueReadOnlyIndexScan(
            TableSchemaAwareIndexStorage schemaAwareIndexStorage,
            Cursor<IndexRow> cursor,
            HybridTimestamp readTimestamp,
            int batchSize,
            List<BinaryRow> result,
            int tableVersion
    ) {
        if (result.size() >= batchSize || !cursor.hasNext()) {
            return nullCompletedFuture();
        }

        IndexRow indexRow = cursor.next();

        RowId rowId = indexRow.rowId();

        return resolvePlainReadResult(rowId, null, readTimestamp).thenComposeAsync(resolvedReadResult -> {
            BinaryRow binaryRow = upgrade(binaryRow(resolvedReadResult), tableVersion);

            if (binaryRow != null && indexRowMatches(indexRow, binaryRow, schemaAwareIndexStorage)) {
                result.add(binaryRow);
            }

            return continueReadOnlyIndexScan(schemaAwareIndexStorage, cursor, readTimestamp, batchSize, result, tableVersion);
        }, scanRequestExecutor);
    }

    /**
     * Index scan loop. Retrieves next row from index, takes locks, fetches associated data row and collects to the result.
     *
     * @param txId Transaction id.
     * @param schemaAwareIndexStorage Index storage.
     * @param indexLocker Index locker.
     * @param indexCursor Index cursor.
     * @param batchSize Batch size.
     * @param result Result collection.
     * @param isUpperBoundAchieved Function to stop on upper bound.
     * @param tableVersion Table schema version at begin timestamp.
     * @return Future.
     */
    private CompletableFuture<Void> continueIndexScan(
            UUID txId,
            TableSchemaAwareIndexStorage schemaAwareIndexStorage,
            SortedIndexLocker indexLocker,
            Cursor<IndexRow> indexCursor,
            int batchSize,
            List<BinaryRow> result,
            Predicate<IndexRow> isUpperBoundAchieved,
            int tableVersion
    ) {
        if (result.size() == batchSize) { // Batch is full, exit loop.
            return nullCompletedFuture();
        }

        return indexLocker.locksForScan(txId, indexCursor)
                .thenCompose(currentRow -> { // Index row S lock
                    if (isUpperBoundAchieved.test(currentRow)) {
                        return nullCompletedFuture(); // End of range reached. Exit loop.
                    }

                    RowId rowId = currentRow.rowId();

                    return lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.S)
                            .thenComposeAsync(rowLock -> { // Table row S lock
                                return resolvePlainReadResult(rowId, txId).thenCompose(resolvedReadResult -> {
                                    BinaryRow binaryRow = upgrade(binaryRow(resolvedReadResult), tableVersion);

                                    if (binaryRow != null && indexRowMatches(currentRow, binaryRow, schemaAwareIndexStorage)) {
                                        result.add(resolvedReadResult.binaryRow());
                                    }

                                    // Proceed scan.
                                    return continueIndexScan(
                                            txId,
                                            schemaAwareIndexStorage,
                                            indexLocker,
                                            indexCursor,
                                            batchSize,
                                            result,
                                            isUpperBoundAchieved,
                                            tableVersion
                                    );
                                });
                            }, scanRequestExecutor);
                });
    }

    /**
     * Checks whether passed index row corresponds to the binary row.
     *
     * @param indexRow Index row, read from index storage.
     * @param binaryRow Binary row, read from MV storage.
     * @param schemaAwareIndexStorage Schema aware index storage, to resolve values of indexed columns in a binary row.
     * @return {@code true} if index row matches the binary row, {@code false} otherwise.
     */
    private static boolean indexRowMatches(IndexRow indexRow, BinaryRow binaryRow, TableSchemaAwareIndexStorage schemaAwareIndexStorage) {
        BinaryTuple actualIndexRow = schemaAwareIndexStorage.indexRowResolver().extractColumns(binaryRow);

        return indexRow.indexColumns().byteBuffer().equals(actualIndexRow.byteBuffer());
    }

    private CompletableFuture<Void> continueIndexLookup(
            UUID txId,
            Cursor<RowId> indexCursor,
            int batchSize,
            List<BinaryRow> result
    ) {
        if (result.size() >= batchSize || !indexCursor.hasNext()) {
            return nullCompletedFuture();
        }

        RowId rowId = indexCursor.next();

        return lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.S)
                .thenComposeAsync(rowLock -> { // Table row S lock
                    return resolvePlainReadResult(rowId, txId).thenCompose(resolvedReadResult -> {
                        if (resolvedReadResult != null && resolvedReadResult.binaryRow() != null) {
                            result.add(resolvedReadResult.binaryRow());
                        }

                        // Proceed lookup.
                        return continueIndexLookup(txId, indexCursor, batchSize, result);
                    });
                }, scanRequestExecutor);
    }

    /**
     * Resolves a result received from a direct storage read.
     *
     * @param rowId Row id to resolve.
     * @param txId Transaction id is used for RW only.
     * @param timestamp Read timestamp.
     * @return Future finishes with the resolved binary row.
     */
    private CompletableFuture<@Nullable TimedBinaryRow> resolvePlainReadResult(
            RowId rowId,
            @Nullable UUID txId,
            @Nullable HybridTimestamp timestamp
    ) {
        ReadResult readResult = mvDataStorage.read(rowId, timestamp == null ? HybridTimestamp.MAX_VALUE : timestamp);

        return resolveReadResult(readResult, txId, timestamp, () -> {
            HybridTimestamp newestCommitTimestamp = readResult.newestCommitTimestamp();

            if (newestCommitTimestamp == null) {
                return null;
            }

            ReadResult committedReadResult = mvDataStorage.read(rowId, newestCommitTimestamp);

            assert !committedReadResult.isWriteIntent() :
                    "The result is not committed [rowId=" + rowId + ", timestamp="
                            + newestCommitTimestamp + ']';

            return new TimedBinaryRow(committedReadResult.binaryRow(), committedReadResult.commitTimestamp());
        });
    }

    /**
     * Resolves a result received from a direct storage read. Use it for RW.
     *
     * @param rowId Row id.
     * @param txId Transaction id.
     * @return Future finishes with the resolved binary row.
     */
    private CompletableFuture<@Nullable TimedBinaryRow> resolvePlainReadResult(RowId rowId, UUID txId) {
        return resolvePlainReadResult(rowId, txId, null).thenCompose(row -> {
            if (row == null || row.binaryRow() == null) {
                return nullCompletedFuture();
            }

            return validateBackwardCompatibility(row.binaryRow(), txId)
                    .thenApply(unused -> row);
        });
    }

    /**
     * Processes transaction cleanup request:
     * <ol>
     *     <li>Waits for finishing of local transactional operations;</li>
     *     <li>Runs asynchronously the specific raft {@code TxCleanupCommand} command, that will convert all pending entries(writeIntents)
     *     to either regular values({@link TxState#COMMITTED}) or removing them ({@link TxState#ABORTED});</li>
     *     <li>Releases all locks that were held on local Replica by given transaction.</li>
     * </ol>
     * This operation is idempotent, so it's safe to retry it.
     *
     * @param request Transaction cleanup request.
     * @return CompletableFuture of ReplicaResult.
     */
    private CompletableFuture<ReplicaResult> processWriteIntentSwitchAction(WriteIntentSwitchReplicaRequest request) {
        // When doing changes to this code, please take a look at WriteIntentSwitchRequestHandler#handle() as it might also need
        // to be touched.

        assert !nodeProperties.colocationEnabled() : request;

        replicaTxFinishMarker.markFinished(request.txId(), request.commit() ? COMMITTED : ABORTED, request.commitTimestamp());

        return awaitCleanupReadyFutures(request.txId(), request.commit())
                .thenApply(res -> {
                    if (res.shouldApplyWriteIntent()) {
                        CompletableFuture<WriteIntentSwitchReplicatedInfo> commandReplicatedFuture =
                                applyWriteIntentSwitchCommandLocallyAndToGroup(request);

                        return new ReplicaResult(null, new CommandApplicationResult(null, commandReplicatedFuture));
                    } else {
                        return new ReplicaResult(writeIntentSwitchReplicatedInfoFor(request), null);
                    }
                });
    }

    private CompletableFuture<ReplicaResult> processTableWriteIntentSwitchAction(TableWriteIntentSwitchReplicaRequest request) {
        assert nodeProperties.colocationEnabled() : request;

        return awaitCleanupReadyFutures(request.txId(), request.commit())
                .thenApply(res -> {
                    if (res.shouldApplyWriteIntent()) {
                        applyWriteIntentSwitchCommandLocally(request);
                    }

                    return new ReplicaResult(res, null);
                });
    }

    private WriteIntentSwitchReplicatedInfo writeIntentSwitchReplicatedInfoFor(WriteIntentSwitchReplicaRequest request) {
        return new WriteIntentSwitchReplicatedInfo(request.txId(), replicationGroupId);
    }

    private CompletableFuture<FuturesCleanupResult> awaitCleanupReadyFutures(UUID txId, boolean commit) {
        List<CompletableFuture<?>> txUpdateFutures = new ArrayList<>();
        List<CompletableFuture<?>> txReadFutures = new ArrayList<>();

        AtomicBoolean forceCleanup = new AtomicBoolean(true);

        txCleanupReadyFutures.compute(txId, (id, txOps) -> {
            if (txOps == null) {
                return null;
            }

            // Cleanup futures (both read and update) are empty in two cases:
            // - there were no actions in the transaction
            // - write intent switch is being executed on the new primary (the primary has changed after write intent appeared)
            // Both cases are expected to happen extremely rarely so we are fine to force the write intent switch.

            // The reason for the forced switch is that otherwise write intents would not be switched (if there is no volatile state and
            // FuturesCleanupResult.hadUpdateFutures() returns false).
            forceCleanup.set(txOps.futures.isEmpty());

            txOps.futures.forEach((opType, futures) -> {
                if (opType.isRwRead()) {
                    txReadFutures.addAll(futures.values());
                } else {
                    txUpdateFutures.addAll(futures.values());
                }
            });

            txOps.futures.clear();

            return null;
        });

        return allOfFuturesExceptionIgnored(txUpdateFutures, commit, txId)
                .thenCompose(v -> allOfFuturesExceptionIgnored(txReadFutures, commit, txId))
                .thenApply(v -> new FuturesCleanupResult(!txReadFutures.isEmpty(), !txUpdateFutures.isEmpty(), forceCleanup.get()));
    }

    private CompletableFuture<WriteIntentSwitchReplicatedInfo> applyWriteIntentSwitchCommandLocallyAndToGroup(
            WriteIntentSwitchReplicaRequest request
    ) {
        applyWriteIntentSwitchCommandLocally(request);

        WriteIntentSwitchReplicatedInfo result = writeIntentSwitchReplicatedInfoFor(request);

        assert !nodeProperties.colocationEnabled() : request;

        @Nullable HybridTimestamp commitTimestamp = request.commitTimestamp();
        HybridTimestamp commandTimestamp = commitTimestamp != null ? commitTimestamp : beginTimestamp(request.txId());

        return reliableCatalogVersions.safeReliableCatalogVersionFor(commandTimestamp)
                .thenCompose(catalogVersion -> applyWriteIntentSwitchCommandToGroup(request, catalogVersion))
                .thenApply(res -> result);
    }

    private void applyWriteIntentSwitchCommandLocally(WriteIntentSwitchReplicaRequestBase request) {
        storageUpdateHandler.switchWriteIntents(
                request.txId(),
                request.commit(),
                request.commitTimestamp(),
                indexIdsAtRwTxBeginTsOrNull(request.txId())
        );
    }

    private CompletableFuture<?> applyWriteIntentSwitchCommandToGroup(WriteIntentSwitchReplicaRequest request, int catalogVersion) {
        WriteIntentSwitchCommand wiSwitchCmd = PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                .txId(request.txId())
                .commit(request.commit())
                .commitTimestamp(request.commitTimestamp())
                .initiatorTime(clockService.current())
                .tableIds(request.tableIds())
                .requiredCatalogVersion(catalogVersion)
                .build();

        return applyCmdWithExceptionHandling(wiSwitchCmd)
                .exceptionally(e -> {
                    if (!ReplicatorRecoverableExceptions.isRecoverable(e)) {
                        LOG.warn("Failed to complete transaction cleanup command [txId=" + request.txId() + ']', e);
                    }

                    ExceptionUtils.sneakyThrow(e);

                    return null;
                });
    }

    /**
     * Creates a future that waits all transaction operations are completed.
     *
     * @param txFutures Transaction operation futures.
     * @param commit If {@code true} this is a commit otherwise a rollback.
     * @param txId Transaction id.
     * @return The future completes when all futures in passed list are completed.
     */
    private static CompletableFuture<Void> allOfFuturesExceptionIgnored(List<CompletableFuture<?>> txFutures, boolean commit, UUID txId) {
        return allOf(txFutures.toArray(new CompletableFuture<?>[0]))
                .exceptionally(e -> {
                    assert !commit :
                            "Transaction is committing, but an operation has completed with exception [txId=" + txId
                                    + ", err=" + e.getMessage() + ']';

                    return null;
                });
    }

    private void releaseTxLocks(UUID txId) {
        lockManager.releaseAll(txId);
    }

    /**
     * Finds the row and its identifier by given pk search row.
     *
     * @param pk Binary Tuple representing a primary key.
     * @param txId An identifier of the transaction regarding which we need to resolve the given row.
     * @param action An action to perform on a resolved row.
     * @param <T> A type of the value returned by action.
     * @return A future object representing the result of the given action.
     */
    private <T> CompletableFuture<T> resolveRowByPk(
            BinaryTuple pk,
            UUID txId,
            IgniteTriFunction<@Nullable RowId, @Nullable BinaryRow, @Nullable HybridTimestamp, CompletableFuture<T>> action
    ) {
        IndexLocker pkLocker = indexesLockers.get().get(pkIndexStorage.get().id());

        assert pkLocker != null;

        CompletableFuture<Void> lockFut = pkLocker.locksForLookupByKey(txId, pk);

        Supplier<CompletableFuture<T>> sup = () -> {
            boolean cursorClosureSetUp = false;
            Cursor<RowId> cursor = null;

            try {
                cursor = getFromPkIndex(pk);

                Cursor<RowId> finalCursor = cursor;
                CompletableFuture<T> resolvingFuture = continueResolvingByPk(cursor, txId, action)
                        .whenComplete((res, ex) -> finalCursor.close());

                cursorClosureSetUp = true;

                return resolvingFuture;
            } finally {
                if (!cursorClosureSetUp && cursor != null) {
                    cursor.close();
                }
            }
        };

        if (isCompletedSuccessfully(lockFut)) {
            return sup.get();
        } else {
            return lockFut.thenCompose(ignored -> sup.get());
        }
    }

    private <T> CompletableFuture<T> continueResolvingByPk(
            Cursor<RowId> cursor,
            UUID txId,
            IgniteTriFunction<@Nullable RowId, @Nullable BinaryRow, @Nullable HybridTimestamp, CompletableFuture<T>> action
    ) {
        if (!cursor.hasNext()) {
            return action.apply(null, null, null);
        }

        RowId rowId = cursor.next();

        return resolvePlainReadResult(rowId, txId).thenCompose(row -> {
            if (row != null && row.binaryRow() != null) {
                return action.apply(rowId, row.binaryRow(), row.commitTimestamp());
            } else {
                return continueResolvingByPk(cursor, txId, action);
            }
        });
    }

    /**
     * Appends an operation to prevent the race between commit/rollback and the operation execution.
     *
     * @param txId Transaction id.
     * @param opId Operation id.
     * @param cmdType Command type.
     * @param full {@code True} if a full transaction and can be immediately committed.
     * @param op Operation closure.
     * @return A future object representing the result of the given operation.
     */
    private <T> CompletableFuture<T> appendTxCommand(
            UUID txId,
            OperationId opId,
            RequestType cmdType,
            boolean full,
            Supplier<CompletableFuture<T>> op
    ) {
        if (full) {
            return op.get().whenComplete((v, th) -> {
                // Fast unlock.
                releaseTxLocks(txId);
            });
        }

        var cleanupReadyFut = new CompletableFuture<Void>();

        txCleanupReadyFutures.compute(txId, (id, txOps) -> {
            // First check whether the transaction has already been finished.
            // And complete cleanupReadyFut with exception if it is the case.
            TxStateMeta txStateMeta = txManager.stateMeta(txId);

            if (txStateMeta == null || isFinalState(txStateMeta.txState()) || txStateMeta.txState() == FINISHING) {
                cleanupReadyFut.completeExceptionally(new Exception());

                return txOps;
            }

            // Otherwise collect cleanupReadyFut in the transaction's futures.
            if (txOps == null) {
                txOps = new TxCleanupReadyFutureList();
            }

            txOps.futures.computeIfAbsent(cmdType, type -> new HashMap<>()).put(opId, cleanupReadyFut);

            return txOps;
        });

        if (cleanupReadyFut.isCompletedExceptionally()) {
            TxStateMeta txStateMeta = txManager.stateMeta(txId);

            TxState txState = txStateMeta == null ? null : txStateMeta.txState();
            boolean isFinishedDueToTimeout = txStateMeta != null
                    && txStateMeta.isFinishedDueToTimeout() != null
                    && txStateMeta.isFinishedDueToTimeout();

            return failedFuture(new TransactionException(
                    isFinishedDueToTimeout ? TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR : TX_ALREADY_FINISHED_ERR,
                    "Transaction is already finished txId=[" + txId + ", txState=" + txState + "]."
            ));
        }

        CompletableFuture<T> fut = op.get();

        fut.whenComplete((v, th) -> {
            if (th != null) {
                cleanupReadyFut.completeExceptionally(th);
            } else {
                if (v instanceof ReplicaResult) {
                    ReplicaResult res = (ReplicaResult) v;

                    if (res.applyResult().replicationFuture() != null) {
                        res.applyResult().replicationFuture().whenComplete((v0, th0) -> {
                            if (th0 != null) {
                                cleanupReadyFut.completeExceptionally(th0);
                            } else {
                                cleanupReadyFut.complete(null);
                            }
                        });
                    } else {
                        cleanupReadyFut.complete(null);
                    }
                } else {
                    cleanupReadyFut.complete(null);
                }
            }
        });

        return fut;
    }

    /**
     * Finds the row and its identifier by given pk search row.
     *
     * @param pk Binary Tuple bytes representing a primary key.
     * @param ts A timestamp regarding which we need to resolve the given row.
     * @return Result of the given action.
     */
    private CompletableFuture<@Nullable BinaryRow> resolveRowByPkForReadOnly(BinaryTuple pk, HybridTimestamp ts) {
        // Indexes store values associated with different versions of one entry.
        // It's possible to have multiple entries for a particular search key
        // only if we insert, delete and again insert an entry with the same indexed fields.
        // It means that there exists one and only one non-empty readResult for any read timestamp for the given key.
        // Which in turn means that if we have found non empty readResult during PK index iteration
        // we can proceed with readResult resolution and stop the iteration.
        try (Cursor<RowId> cursor = getFromPkIndex(pk)) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-18767 scan of multiple write intents should not be needed
            List<ReadResult> writeIntents = new ArrayList<>();
            List<ReadResult> regularEntries = new ArrayList<>();

            for (RowId rowId : cursor) {
                ReadResult readResult = mvDataStorage.read(rowId, ts);

                if (readResult.isWriteIntent()) {
                    writeIntents.add(readResult);
                } else if (!readResult.isEmpty()) {
                    regularEntries.add(readResult);
                }
            }

            // Nothing found in the storage, return null.
            if (writeIntents.isEmpty() && regularEntries.isEmpty()) {
                metrics.onRead(true);

                return nullCompletedFuture();
            }

            if (writeIntents.isEmpty()) {
                metrics.onRead(true);

                // No write intents, then return the committed value. We already know that regularEntries is not empty.
                return completedFuture(regularEntries.get(0).binaryRow());
            } else {
                ReadResult writeIntent = writeIntents.get(0);

                // Assume that all write intents for the same key belong to the same transaction, as the key should be exclusively locked.
                // This means that we can just resolve the state of this transaction.
                checkWriteIntentsBelongSameTx(writeIntents);

                return inBusyLockAsync(busyLock, () ->
                        resolveWriteIntentReadability(writeIntent, ts)
                                .thenApply(writeIntentReadable ->
                                        inBusyLock(busyLock, () -> {
                                            metrics.onRead(true);

                                            if (writeIntentReadable) {
                                                return findAny(writeIntents, wi -> !wi.isEmpty()).map(ReadResult::binaryRow).orElse(null);
                                            } else {
                                                for (ReadResult wi : writeIntents) {
                                                    HybridTimestamp newestCommitTimestamp = wi.newestCommitTimestamp();

                                                    if (newestCommitTimestamp == null) {
                                                        continue;
                                                    }

                                                    ReadResult committedReadResult = mvDataStorage.read(wi.rowId(), newestCommitTimestamp);

                                                    assert !committedReadResult.isWriteIntent() :
                                                            "The result is not committed [rowId=" + wi.rowId() + ", timestamp="
                                                                    + newestCommitTimestamp + ']';

                                                    return committedReadResult.binaryRow();
                                                }

                                                // No suitable value found in write intents, read the committed value (if exists)
                                                return findFirst(regularEntries).map(ReadResult::binaryRow).orElse(null);
                                            }
                                        }))
                );
            }
        }
    }

    /**
     * Check that all given write intents belong to the same transaction.
     *
     * @param writeIntents Write intents.
     */
    private static void checkWriteIntentsBelongSameTx(Collection<ReadResult> writeIntents) {
        ReadResult writeIntent = findAny(writeIntents).orElseThrow();

        for (ReadResult wi : writeIntents) {
            assert Objects.equals(wi.transactionId(), writeIntent.transactionId())
                    : "Unexpected write intent, tx1=" + writeIntent.transactionId() + ", tx2=" + wi.transactionId();

            assert Objects.equals(wi.commitTableOrZoneId(), writeIntent.commitTableOrZoneId())
                    : "Unexpected write intent, commitTableOrZoneId1=" + writeIntent.commitTableOrZoneId()
                    + ", commitTableId2=" + wi.commitTableOrZoneId();

            assert wi.commitPartitionId() == writeIntent.commitPartitionId()
                    : "Unexpected write intent, commitPartitionId1=" + writeIntent.commitPartitionId()
                    + ", commitPartitionId2=" + wi.commitPartitionId();
        }
    }

    /**
     * Tests row values for equality.
     *
     * @param row Row.
     * @param row2 Row.
     * @return {@code true} if rows are equal.
     */
    private static boolean equalValues(BinaryRow row, BinaryRow row2) {
        return row.tupleSlice().compareTo(row2.tupleSlice()) == 0;
    }

    /**
     * Processes multiple entries direct request for read only transaction.
     *
     * @param request Read only multiple entries request.
     * @param opStartTimestamp Moment when the operation processing was started in this class.
     * @return Result future.
     */
    private CompletableFuture<List<BinaryRow>> processReadOnlyDirectMultiEntryAction(
            ReadOnlyDirectMultiRowReplicaRequest request,
            HybridTimestamp opStartTimestamp) {
        List<BinaryTuple> primaryKeys = resolvePks(request.primaryKeys());

        assert request.requestType() == RO_GET_ALL;

        CompletableFuture<BinaryRow>[] resolutionFuts = new CompletableFuture[primaryKeys.size()];

        for (int i = 0; i < primaryKeys.size(); i++) {
            resolutionFuts[i] = resolveRowByPkForReadOnly(primaryKeys.get(i), opStartTimestamp);
        }

        return allOfToList(resolutionFuts).thenApply(rows -> {
            // Validate read correctness.
            HybridTimestamp lwm = lowWatermark.getLowWatermark();

            if (lwm != null && opStartTimestamp.compareTo(lwm) < 0) {
                throw new IgniteInternalException(
                        TX_READ_ONLY_TOO_OLD_ERR,
                        "Attempted to read data below the garbage collection watermark: [readTimestamp={}, gcTimestamp={}]",
                        opStartTimestamp,
                        lowWatermark.getLowWatermark());
            }

            return rows;
        });
    }

    /**
     * Precesses multi request.
     *
     * @param request Multi request operation.
     * @param leaseStartTime Lease start time.
     * @return Listener response.
     */
    private CompletableFuture<ReplicaResult> processMultiEntryAction(ReadWriteMultiRowReplicaRequest request, long leaseStartTime) {
        UUID txId = request.transactionId();
        ReplicationGroupId commitPartitionId = request.commitPartitionId().asReplicationGroupId();
        List<BinaryRow> searchRows = request.binaryRows();

        assert commitPartitionId != null : "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_DELETE_EXACT_ALL: {
                CompletableFuture<RowId>[] deleteExactLockFuts = new CompletableFuture[searchRows.size()];

                Map<UUID, HybridTimestamp> lastCommitTimes = new ConcurrentHashMap<>();

                for (int i = 0; i < searchRows.size(); i++) {
                    BinaryRow searchRow = searchRows.get(i);

                    deleteExactLockFuts[i] = resolveRowByPk(extractPk(searchRow), txId, (rowId, row, lastCommitTime) -> {
                        if (rowId == null) {
                            return nullCompletedFuture();
                        }

                        if (lastCommitTime != null) {
                            lastCommitTimes.put(rowId.uuid(), lastCommitTime);
                        }

                        return takeLocksForDeleteExact(searchRow, rowId, row, txId);
                    });
                }

                return allOf(deleteExactLockFuts).thenCompose(ignore -> {
                    Map<UUID, TimedBinaryRowMessage> rowIdsToDelete = new HashMap<>();
                    // TODO:IGNITE-20669 Replace the result to BitSet.
                    Collection<BinaryRow> result = new ArrayList<>();
                    List<RowId> rows = new ArrayList<>();

                    for (int i = 0; i < searchRows.size(); i++) {
                        RowId lockedRowId = deleteExactLockFuts[i].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.put(lockedRowId.uuid(), PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                                    .timestamp(lastCommitTimes.get(lockedRowId.uuid()))
                                    .build());

                            result.add(new NullBinaryRow());

                            rows.add(lockedRowId);
                        } else {
                            result.add(null);
                        }
                    }

                    if (rowIdsToDelete.isEmpty()) {
                        metrics.onRead(searchRows.size(), false);

                        return completedFuture(new ReplicaResult(result, null));
                    }

                    return validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                            .thenCompose(catalogVersion -> awaitCleanup(rows, catalogVersion))
                            .thenCompose(
                                    catalogVersion -> applyUpdateAllCommand(
                                            request,
                                            rowIdsToDelete,
                                            catalogVersion,
                                            leaseStartTime
                                    )
                            )
                            .thenApply(res -> {
                                metrics.onRead(searchRows.size(), false);
                                metrics.onWrite(rowIdsToDelete.size());

                                return new ReplicaResult(result, res);
                            });
                });
            }
            case RW_INSERT_ALL: {
                List<BinaryTuple> pks = new ArrayList<>(searchRows.size());

                CompletableFuture<RowId>[] pkReadLockFuts = new CompletableFuture[searchRows.size()];

                for (int i = 0; i < searchRows.size(); i++) {
                    BinaryTuple pk = extractPk(searchRows.get(i));

                    pks.add(pk);

                    pkReadLockFuts[i] = resolveRowByPk(pk, txId, (rowId, row, lastCommitTime) -> completedFuture(rowId));
                }

                return allOf(pkReadLockFuts).thenCompose(ignore -> {
                    // TODO:IGNITE-20669 Replace the result to BitSet.
                    Collection<BinaryRow> result = new ArrayList<>();
                    Map<RowId, BinaryRow> rowsToInsert = new HashMap<>();
                    Set<ByteBuffer> uniqueKeys = new HashSet<>();

                    for (int i = 0; i < searchRows.size(); i++) {
                        BinaryRow row = searchRows.get(i);
                        RowId lockedRow = pkReadLockFuts[i].join();

                        if (lockedRow == null && uniqueKeys.add(pks.get(i).byteBuffer())) {
                            rowsToInsert.put(new RowId(partId(), RowIdGenerator.next()), row);

                            result.add(new NullBinaryRow());
                        } else {
                            result.add(null);
                        }
                    }

                    if (rowsToInsert.isEmpty()) {
                        metrics.onRead(searchRows.size(), false);

                        return completedFuture(new ReplicaResult(result, null));
                    }

                    CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>>[] insertLockFuts = new CompletableFuture[rowsToInsert.size()];

                    int idx = 0;

                    for (Map.Entry<RowId, BinaryRow> entry : rowsToInsert.entrySet()) {
                        insertLockFuts[idx++] = takeLocksForInsert(entry.getValue(), entry.getKey(), txId);
                    }

                    Map<UUID, TimedBinaryRowMessage> convertedMap = rowsToInsert.entrySet().stream()
                            .collect(toMap(
                                    e -> e.getKey().uuid(),
                                    e -> PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                                            .binaryRowMessage(binaryRowMessage(e.getValue()))
                                            .build()
                            ));

                    return allOf(insertLockFuts)
                            .thenCompose(ignored ->
                                    // We are inserting completely new rows - no need to cleanup anything in this case, hence empty times.
                                    validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                            )
                            .thenCompose(catalogVersion -> applyUpdateAllCommand(
                                            request,
                                            convertedMap,
                                            catalogVersion,
                                            leaseStartTime
                                    )
                            )
                            .thenApply(res -> {
                                metrics.onRead(searchRows.size(), false);
                                metrics.onWrite(rowsToInsert.size());

                                // Release short term locks.
                                for (CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> insertLockFut : insertLockFuts) {
                                    insertLockFut.join().get2()
                                            .forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));
                                }

                                return new ReplicaResult(result, res);
                            });
                });
            }
            case RW_UPSERT_ALL: {
                CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>>[] rowIdFuts = new CompletableFuture[searchRows.size()];
                BinaryTuple[] pks = new BinaryTuple[searchRows.size()];

                Map<UUID, HybridTimestamp> lastCommitTimes = new ConcurrentHashMap<>();
                BitSet deleted = request.deleted();

                // When the same key is updated multiple times within the same batch, we need to maintain operation order and apply
                // only the last update. This map stores the previous searchRows index for each key.
                Map<ByteBuffer, Integer> prevRowIdx = new HashMap<>();

                for (int i = 0; i < searchRows.size(); i++) {
                    BinaryRow searchRow = searchRows.get(i);
                    boolean isDelete = deleted != null && deleted.get(i);

                    BinaryTuple pk = isDelete
                            ? resolvePk(searchRow.tupleSlice())
                            : extractPk(searchRow);

                    pks[i] = pk;

                    Integer prevRowIdx0 = prevRowIdx.put(pk.byteBuffer(), i);
                    if (prevRowIdx0 != null) {
                        rowIdFuts[prevRowIdx0] = nullCompletedFuture(); // Skip previous row with the same key.
                    }
                }

                int uniqueKeysCount = 0;
                for (int i = 0; i < searchRows.size(); i++) {
                    if (rowIdFuts[i] != null) {
                        continue; // Skip previous row with the same key.
                    }

                    uniqueKeysCount++;

                    BinaryRow searchRow = searchRows.get(i);
                    boolean isDelete = deleted != null && deleted.get(i);

                    rowIdFuts[i] = resolveRowByPk(pks[i], txId, (rowId, row, lastCommitTime) -> {
                        if (isDelete && rowId == null) {
                            return nullCompletedFuture();
                        }

                        if (lastCommitTime != null) {
                            //noinspection DataFlowIssue (rowId is not null if lastCommitTime is not null)
                            lastCommitTimes.put(rowId.uuid(), lastCommitTime);
                        }

                        if (isDelete) {
                            assert row != null;

                            return takeLocksForDelete(row, rowId, txId)
                                    .thenApply(id -> new IgniteBiTuple<>(id, null));
                        }

                        boolean insert = rowId == null;
                        RowId rowId0 = insert ? new RowId(partId(), RowIdGenerator.next()) : rowId;

                        return insert
                                ? takeLocksForInsert(searchRow, rowId0, txId)
                                : takeLocksForUpdate(searchRow, rowId0, txId);
                    });
                }

                int uniqueKeysCountFinal = uniqueKeysCount;

                return allOf(rowIdFuts).thenCompose(ignore -> {
                    Map<UUID, TimedBinaryRowMessage> rowsToUpdate = IgniteUtils.newHashMap(searchRows.size());
                    List<RowId> rows = new ArrayList<>();

                    for (int i = 0; i < searchRows.size(); i++) {
                        IgniteBiTuple<RowId, Collection<Lock>> locks = rowIdFuts[i].join();
                        if (locks == null) {
                            continue;
                        }

                        RowId lockedRow = locks.get1();

                        TimedBinaryRowMessageBuilder timedBinaryRowMessageBuilder = PARTITION_REPLICATION_MESSAGES_FACTORY
                                .timedBinaryRowMessage()
                                .timestamp(lastCommitTimes.get(lockedRow.uuid()));

                        if (deleted == null || !deleted.get(i)) {
                            timedBinaryRowMessageBuilder.binaryRowMessage(binaryRowMessage(searchRows.get(i)));
                        }

                        rowsToUpdate.put(lockedRow.uuid(), timedBinaryRowMessageBuilder.build());

                        rows.add(lockedRow);
                    }

                    if (rowsToUpdate.isEmpty()) {
                        metrics.onRead(uniqueKeysCountFinal, false);

                        return completedFuture(new ReplicaResult(null, null));
                    }

                    return validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                            .thenCompose(catalogVersion -> awaitCleanup(rows, catalogVersion))
                            .thenCompose(
                                    catalogVersion -> applyUpdateAllCommand(
                                            request,
                                            rowsToUpdate,
                                            catalogVersion,
                                            leaseStartTime
                                    )
                            )
                            .thenApply(res -> {
                                metrics.onRead(uniqueKeysCountFinal, false);
                                metrics.onWrite(uniqueKeysCountFinal);

                                // Release short term locks.
                                for (CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> rowIdFut : rowIdFuts) {
                                    IgniteBiTuple<RowId, Collection<Lock>> futRes = rowIdFut.join();
                                    Collection<Lock> locks = futRes == null ? null : futRes.get2();

                                    if (locks != null) {
                                        locks.forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));
                                    }
                                }

                                return new ReplicaResult(null, res);
                            });
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        format("Unknown multi request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Precesses multi request.
     *
     * @param request Multi request operation.
     * @param leaseStartTime Lease start time.
     * @return Listener response.
     */
    private CompletableFuture<ReplicaResult> processMultiEntryAction(ReadWriteMultiRowPkReplicaRequest request, long leaseStartTime) {
        UUID txId = request.transactionId();
        ReplicationGroupId commitPartitionId = request.commitPartitionId().asReplicationGroupId();
        List<BinaryTuple> primaryKeys = resolvePks(request.primaryKeys());

        assert commitPartitionId != null || request.requestType() == RW_GET_ALL
                : "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_GET_ALL: {
                CompletableFuture<BinaryRow>[] rowFuts = new CompletableFuture[primaryKeys.size()];

                for (int i = 0; i < primaryKeys.size(); i++) {
                    rowFuts[i] = resolveRowByPk(primaryKeys.get(i), txId, (rowId, row, lastCommitTime) -> {
                        if (rowId == null) {
                            return nullCompletedFuture();
                        }

                        return takeLocksForGet(rowId, txId)
                                .thenApply(ignored -> row);
                    });
                }

                return allOf(rowFuts)
                        .thenCompose(ignored -> {
                            var result = new ArrayList<BinaryRow>(primaryKeys.size());

                            for (CompletableFuture<BinaryRow> rowFut : rowFuts) {
                                result.add(rowFut.join());
                            }

                            if (allElementsAreNull(result)) {
                                metrics.onRead(result.size(), false);

                                return completedFuture(new ReplicaResult(result, null));
                            }

                            return validateRwReadAgainstSchemaAfterTakingLocks(txId)
                                    .thenApply(unused -> {
                                        metrics.onRead(result.size(), false);

                                        return new ReplicaResult(result, null);
                                    });
                        });
            }
            case RW_DELETE_ALL: {
                CompletableFuture<RowId>[] rowIdLockFuts = new CompletableFuture[primaryKeys.size()];

                Map<UUID, HybridTimestamp> lastCommitTimes = new ConcurrentHashMap<>();

                for (int i = 0; i < primaryKeys.size(); i++) {
                    rowIdLockFuts[i] = resolveRowByPk(primaryKeys.get(i), txId, (rowId, row, lastCommitTime) -> {
                        if (rowId == null) {
                            return nullCompletedFuture();
                        }

                        if (lastCommitTime != null) {
                            lastCommitTimes.put(rowId.uuid(), lastCommitTime);
                        }

                        return takeLocksForDelete(row, rowId, txId);
                    });
                }

                return allOf(rowIdLockFuts).thenCompose(ignore -> {
                    Map<UUID, TimedBinaryRowMessage> rowIdsToDelete = new HashMap<>();
                    // TODO:IGNITE-20669 Replace the result to BitSet.
                    Collection<BinaryRow> result = new ArrayList<>();
                    List<RowId> rows = new ArrayList<>();

                    for (CompletableFuture<RowId> lockFut : rowIdLockFuts) {
                        RowId lockedRowId = lockFut.join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.put(lockedRowId.uuid(), PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                                    .timestamp(lastCommitTimes.get(lockedRowId.uuid()))
                                    .build());

                            rows.add(lockedRowId);

                            result.add(new NullBinaryRow());
                        } else {
                            result.add(null);
                        }
                    }

                    if (rowIdsToDelete.isEmpty()) {
                        return completedFuture(new ReplicaResult(result, null));
                    }

                    return validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                            .thenCompose(catalogVersion -> awaitCleanup(rows, catalogVersion))
                            .thenCompose(
                                    catalogVersion -> applyUpdateAllCommand(
                                            rowIdsToDelete,
                                            request.commitPartitionId(),
                                            request.transactionId(),
                                            request.full(),
                                            request.coordinatorId(),
                                            catalogVersion,
                                            request.skipDelayedAck(),
                                            leaseStartTime
                                    )
                            )
                            .thenApply(res -> {
                                metrics.onWrite(rowIdsToDelete.size());

                                return new ReplicaResult(result, res);
                            });
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        format("Unknown multi request [actionType={}]", request.requestType()));
            }
        }
    }

    private static <T> boolean allElementsAreNull(List<T> list) {
        for (T element : list) {
            if (element != null) {
                return false;
            }
        }

        return true;
    }

    private CompletableFuture<Object> applyCmdWithExceptionHandling(Command cmd) {
        return raftCommandApplicator.applyCommandWithExceptionHandling(cmd);
    }

    /**
     * Executes an Update command.
     *
     * @param commitPartitionId Commit partition ID.
     * @param rowUuid Row UUID.
     * @param row Row.
     * @param lastCommitTimestamp The timestamp of the last committed entry for the row.
     * @param txId Transaction ID.
     * @param full {@code True} if this is a full transaction.
     * @param txCoordinatorId Transaction coordinator id.
     * @param catalogVersion Validated catalog version associated with given operation.
     * @param leaseStartTime Lease start time.
     * @param skipDelayedAck {@code True} to skip delayed ack optimization.
     * @return A local update ready future, possibly having a nested replication future as a result for delayed ack purpose.
     */
    private CompletableFuture<CommandApplicationResult> applyUpdateCommand(
            ReplicationGroupId commitPartitionId,
            UUID rowUuid,
            @Nullable BinaryRow row,
            @Nullable HybridTimestamp lastCommitTimestamp,
            UUID txId,
            boolean full,
            UUID txCoordinatorId,
            int catalogVersion,
            boolean skipDelayedAck,
            long leaseStartTime
    ) {
        UpdateCommand cmd = updateCommand(
                commitPartitionId,
                rowUuid,
                row,
                lastCommitTimestamp,
                txId,
                full,
                txCoordinatorId,
                clockService.current(),
                catalogVersion,
                full ? leaseStartTime : null  // Lease start time check within the replication group is needed only for full txns.
        );

        if (!cmd.full()) {
            if (skipDelayedAck) {
                if (!SKIP_UPDATES) {
                    storageUpdateHandler.handleUpdate(
                            cmd.txId(),
                            cmd.rowUuid(),
                            cmd.commitPartitionId().asReplicationGroupId(),
                            cmd.rowToUpdate(),
                            true,
                            null,
                            null,
                            null,
                            indexIdsAtRwTxBeginTs(txId)
                    );
                }

                return applyCmdWithExceptionHandling(cmd).thenApply(res -> null);
            } else {
                if (!SKIP_UPDATES) {
                    // We don't need to take the partition snapshots read lock, see #INTERNAL_DOC_PLACEHOLDER why.
                    storageUpdateHandler.handleUpdate(
                            cmd.txId(),
                            cmd.rowUuid(),
                            cmd.commitPartitionId().asReplicationGroupId(),
                            cmd.rowToUpdate(),
                            true,
                            null,
                            null,
                            null,
                            indexIdsAtRwTxBeginTs(txId)
                    );
                }

                CompletableFuture<UUID> repFut = applyCmdWithExceptionHandling(cmd).thenApply(res -> cmd.txId());

                return completedFuture(new CommandApplicationResult(null, repFut));
            }
        } else {
            return applyCmdWithExceptionHandling(cmd).thenCompose(res -> {
                UpdateCommandResult updateCommandResult = (UpdateCommandResult) res;

                if (updateCommandResult != null && !updateCommandResult.isPrimaryReplicaMatch()) {
                    throw new PrimaryReplicaMissException(txId, cmd.leaseStartTime(), updateCommandResult.currentLeaseStartTime());
                }

                if (updateCommandResult != null && updateCommandResult.isPrimaryInPeersAndLearners()) {
                    HybridTimestamp safeTs = hybridTimestamp(updateCommandResult.safeTimestamp());
                    return safeTime.waitFor(safeTs)
                            .thenApply(ignored -> new CommandApplicationResult(safeTs, null));
                } else {
                    HybridTimestamp safeTs = updateCommandResult == null ? null : hybridTimestamp(updateCommandResult.safeTimestamp());

                    if (!SKIP_UPDATES) {
                        // We don't need to take the partition snapshots read lock, see #INTERNAL_DOC_PLACEHOLDER why.
                        storageUpdateHandler.handleUpdate(
                                cmd.txId(),
                                cmd.rowUuid(),
                                cmd.commitPartitionId().asReplicationGroupId(),
                                cmd.rowToUpdate(),
                                false,
                                null,
                                safeTs,
                                null,
                                indexIdsAtRwTxBeginTs(txId)
                        );
                    }

                    return completedFuture(new CommandApplicationResult(safeTs, null));
                }
            });
        }
    }

    /**
     * Executes an Update command.
     *
     * @param request Read write single row replica request.
     * @param rowUuid Row UUID.
     * @param row Row.
     * @param lastCommitTimestamp The timestamp of the last committed entry for the row.
     * @param catalogVersion Validated catalog version associated with given operation.
     * @param leaseStartTime Lease start time.
     * @return A local update ready future, possibly having a nested replication future as a result for delayed ack purpose.
     */
    private CompletableFuture<CommandApplicationResult> applyUpdateCommand(
            ReadWriteSingleRowReplicaRequest request,
            UUID rowUuid,
            @Nullable BinaryRow row,
            @Nullable HybridTimestamp lastCommitTimestamp,
            int catalogVersion,
            long leaseStartTime
    ) {
        return applyUpdateCommand(
                request.commitPartitionId().asReplicationGroupId(),
                rowUuid,
                row,
                lastCommitTimestamp,
                request.transactionId(),
                request.full(),
                request.coordinatorId(),
                catalogVersion,
                request.skipDelayedAck(),
                leaseStartTime
        );
    }

    /**
     * Executes an UpdateAll command.
     *
     * @param rowsToUpdate All {@link BinaryRow}s represented as {@link TimedBinaryRowMessage}s to be updated.
     * @param commitPartitionId Partition ID that these rows belong to.
     * @param txId Transaction ID.
     * @param full {@code true} if this is a single-command transaction.
     * @param txCoordinatorId Transaction coordinator id.
     * @param catalogVersion Validated catalog version associated with given operation.
     * @param skipDelayedAck {@code true} to disable the delayed ack optimization.
     * @return Raft future, see {@link #applyCmdWithExceptionHandling(Command)}.
     */
    private CompletableFuture<CommandApplicationResult> applyUpdateAllCommand(
            Map<UUID, TimedBinaryRowMessage> rowsToUpdate,
            ReplicationGroupIdMessage commitPartitionId,
            UUID txId,
            boolean full,
            UUID txCoordinatorId,
            int catalogVersion,
            boolean skipDelayedAck,
            long leaseStartTime
    ) {
        UpdateAllCommand cmd = updateAllCommand(
                rowsToUpdate,
                commitPartitionId,
                txId,
                clockService.current(),
                full,
                txCoordinatorId,
                catalogVersion,
                full ? leaseStartTime : null  // Lease start time check within the replication group is needed only for full txns.
        );

        if (!cmd.full()) {
            if (skipDelayedAck) {
                // We don't need to take the partition snapshots read lock, see #INTERNAL_DOC_PLACEHOLDER why.
                storageUpdateHandler.handleUpdateAll(
                        cmd.txId(),
                        cmd.rowsToUpdate(),
                        cmd.commitPartitionId().asReplicationGroupId(),
                        true,
                        null,
                        null,
                        indexIdsAtRwTxBeginTs(txId)
                );

                return applyCmdWithExceptionHandling(cmd).thenApply(res -> null);
            } else {
                // We don't need to take the partition snapshots read lock, see #INTERNAL_DOC_PLACEHOLDER why.
                storageUpdateHandler.handleUpdateAll(
                        cmd.txId(),
                        cmd.rowsToUpdate(),
                        cmd.commitPartitionId().asReplicationGroupId(),
                        true,
                        null,
                        null,
                        indexIdsAtRwTxBeginTs(txId)
                );
            }

            CompletableFuture<UUID> repFut = applyCmdWithExceptionHandling(cmd).thenApply(res -> cmd.txId());

            return completedFuture(new CommandApplicationResult(null, repFut));
        } else {
            return applyCmdWithExceptionHandling(cmd).thenCompose(res -> {
                UpdateCommandResult updateCommandResult = (UpdateCommandResult) res;

                if (!updateCommandResult.isPrimaryReplicaMatch()) {
                    throw new PrimaryReplicaMissException(
                            cmd.txId(),
                            cmd.leaseStartTime(),
                            updateCommandResult.currentLeaseStartTime()
                    );
                }
                if (updateCommandResult.isPrimaryInPeersAndLearners()) {
                    HybridTimestamp safeTs = hybridTimestamp(updateCommandResult.safeTimestamp());
                    return safeTime.waitFor(safeTs)
                            .thenApply(ignored -> new CommandApplicationResult(safeTs, null));
                } else {
                    HybridTimestamp safeTs = hybridTimestamp(updateCommandResult.safeTimestamp());

                    // We don't need to take the partition snapshots read lock, see #INTERNAL_DOC_PLACEHOLDER why.
                    storageUpdateHandler.handleUpdateAll(
                            cmd.txId(),
                            cmd.rowsToUpdate(),
                            cmd.commitPartitionId().asReplicationGroupId(),
                            false,
                            null,
                            safeTs,
                            indexIdsAtRwTxBeginTs(txId)
                    );

                    return completedFuture(new CommandApplicationResult(safeTs, null));
                }
            });
        }
    }

    /**
     * Executes an UpdateAll command.
     *
     * @param request Read write multi rows replica request.
     * @param rowsToUpdate All {@link BinaryRow}s represented as {@link TimedBinaryRowMessage}s to be updated.
     * @param catalogVersion Validated catalog version associated with given operation.
     * @param leaseStartTime Lease start time.
     * @return Raft future.
     */
    private CompletableFuture<CommandApplicationResult> applyUpdateAllCommand(
            ReadWriteMultiRowReplicaRequest request,
            Map<UUID, TimedBinaryRowMessage> rowsToUpdate,
            int catalogVersion,
            long leaseStartTime
    ) {
        return applyUpdateAllCommand(
                rowsToUpdate,
                request.commitPartitionId(),
                request.transactionId(),
                request.full(),
                request.coordinatorId(),
                catalogVersion,
                request.skipDelayedAck(),
                leaseStartTime
        );
    }

    /**
     * Processes single entry direct request for read only transaction.
     *
     * @param request Read only single entry request.
     * @param opStartTimestamp Moment when the operation processing was started in this class.
     * @return Result future.
     */
    private CompletableFuture<BinaryRow> processReadOnlyDirectSingleEntryAction(
            ReadOnlyDirectSingleRowReplicaRequest request,
            HybridTimestamp opStartTimestamp
    ) {
        BinaryTuple primaryKey = resolvePk(request.primaryKey());
        HybridTimestamp readTimestamp = opStartTimestamp;

        if (request.requestType() != RO_GET) {
            throw new IgniteInternalException(
                    Replicator.REPLICA_COMMON_ERR,
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        return resolveRowByPkForReadOnly(primaryKey, readTimestamp);
    }

    /**
     * Precesses single request.
     *
     * @param request Single request operation.
     * @param leaseStartTime Lease start time.
     * @return Listener response.
     */
    private CompletableFuture<ReplicaResult> processSingleEntryAction(ReadWriteSingleRowReplicaRequest request, long leaseStartTime) {
        UUID txId = request.transactionId();
        BinaryRow searchRow = request.binaryRow();
        ReplicationGroupId commitPartitionId = request.commitPartitionId().asReplicationGroupId();

        assert commitPartitionId != null : "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_DELETE_EXACT: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row, lastCommitTime) -> {
                    if (rowId == null) {
                        metrics.onRead(false);

                        return completedFuture(new ReplicaResult(false, null));
                    }

                    return takeLocksForDeleteExact(searchRow, rowId, row, txId)
                            .thenCompose(validatedRowId -> {
                                if (validatedRowId == null) {
                                    metrics.onRead(false);

                                    return completedFuture(new ReplicaResult(false, null));
                                }

                                return validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                                        .thenCompose(catalogVersion -> awaitCleanup(validatedRowId, catalogVersion))
                                        .thenCompose(
                                                catalogVersion -> applyUpdateCommand(
                                                        request,
                                                        validatedRowId.uuid(),
                                                        null,
                                                        lastCommitTime,
                                                        catalogVersion,
                                                        leaseStartTime
                                                )
                                        )
                                        .thenApply(res -> {
                                            metrics.onRead(false);
                                            metrics.onWrite();

                                            return new ReplicaResult(true, res);
                                        });
                            });
                });
            }
            case RW_INSERT: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row, lastCommitTime) -> {
                    if (rowId != null) {
                        metrics.onRead(false);

                        return completedFuture(new ReplicaResult(false, null));
                    }

                    RowId rowId0 = new RowId(partId(), RowIdGenerator.next());

                    return takeLocksForInsert(searchRow, rowId0, txId)
                            .thenCompose(rowIdLock -> validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                                    .thenCompose(
                                            catalogVersion -> applyUpdateCommand(
                                                    request,
                                                    rowId0.uuid(),
                                                    searchRow,
                                                    lastCommitTime,
                                                    catalogVersion,
                                                    leaseStartTime
                                            )
                                    )
                                    .thenApply(res -> new IgniteBiTuple<>(res, rowIdLock)))
                            .thenApply(tuple -> {
                                metrics.onRead(false);
                                metrics.onWrite();

                                // Release short term locks.
                                tuple.get2().get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                return new ReplicaResult(true, tuple.get1());
                            });
                });
            }
            case RW_UPSERT: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row, lastCommitTime) -> {
                    boolean insert = rowId == null;

                    RowId rowId0 = insert ? new RowId(partId(), RowIdGenerator.next()) : rowId;

                    CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> lockFut = insert
                            ? takeLocksForInsert(searchRow, rowId0, txId)
                            : takeLocksForUpdate(searchRow, rowId0, txId);

                    return lockFut
                            .thenCompose(rowIdLock -> validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                                    .thenCompose(catalogVersion -> awaitCleanup(rowId, catalogVersion))
                                    .thenCompose(
                                            catalogVersion -> applyUpdateCommand(
                                                    request,
                                                    rowId0.uuid(),
                                                    searchRow,
                                                    lastCommitTime,
                                                    catalogVersion,
                                                    leaseStartTime
                                            )
                                    )
                                    .thenApply(res -> new IgniteBiTuple<>(res, rowIdLock)))
                            .thenApply(tuple -> {
                                metrics.onWrite();

                                // Release short term locks.
                                tuple.get2().get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                return new ReplicaResult(null, tuple.get1());
                            });
                });
            }
            case RW_GET_AND_UPSERT: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row, lastCommitTime) -> {
                    boolean insert = rowId == null;

                    RowId rowId0 = insert ? new RowId(partId(), RowIdGenerator.next()) : rowId;

                    CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> lockFut = insert
                            ? takeLocksForInsert(searchRow, rowId0, txId)
                            : takeLocksForUpdate(searchRow, rowId0, txId);

                    return lockFut
                            .thenCompose(rowIdLock -> validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                                    .thenCompose(catalogVersion -> awaitCleanup(rowId, catalogVersion))
                                    .thenCompose(
                                            catalogVersion -> applyUpdateCommand(
                                                    request,
                                                    rowId0.uuid(),
                                                    searchRow,
                                                    lastCommitTime,
                                                    catalogVersion,
                                                    leaseStartTime
                                            )
                                    )
                                    .thenApply(res -> new IgniteBiTuple<>(res, rowIdLock)))
                            .thenApply(tuple -> {
                                metrics.onRead(false);
                                metrics.onWrite();

                                // Release short term locks.
                                tuple.get2().get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                return new ReplicaResult(row, tuple.get1());
                            });
                });
            }
            case RW_GET_AND_REPLACE: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row, lastCommitTime) -> {
                    if (rowId == null) {
                        metrics.onRead(false);

                        return completedFuture(new ReplicaResult(null, null));
                    }

                    return takeLocksForUpdate(searchRow, rowId, txId)
                            .thenCompose(rowIdLock -> validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                                    .thenCompose(catalogVersion -> awaitCleanup(rowId, catalogVersion))
                                    .thenCompose(
                                            catalogVersion -> applyUpdateCommand(
                                                    request,
                                                    rowId.uuid(),
                                                    searchRow,
                                                    lastCommitTime,
                                                    catalogVersion,
                                                    leaseStartTime
                                            )
                                    )
                                    .thenApply(res -> new IgniteBiTuple<>(res, rowIdLock)))
                            .thenApply(tuple -> {
                                metrics.onRead(false);
                                metrics.onWrite();

                                // Release short term locks.
                                tuple.get2().get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                return new ReplicaResult(row, tuple.get1());
                            });
                });
            }
            case RW_REPLACE_IF_EXIST: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row, lastCommitTime) -> {
                    if (rowId == null) {
                        metrics.onRead(false);

                        return completedFuture(new ReplicaResult(false, null));
                    }

                    return takeLocksForUpdate(searchRow, rowId, txId)
                            .thenCompose(rowIdLock -> validateWriteAgainstSchemaAfterTakingLocks(request.transactionId())
                                    .thenCompose(catalogVersion -> awaitCleanup(rowId, catalogVersion))
                                    .thenCompose(
                                            catalogVersion -> applyUpdateCommand(
                                                    request,
                                                    rowId.uuid(),
                                                    searchRow,
                                                    lastCommitTime,
                                                    catalogVersion,
                                                    leaseStartTime
                                            )
                                    )
                                    .thenApply(res -> new IgniteBiTuple<>(res, rowIdLock)))
                            .thenApply(tuple -> {
                                metrics.onRead(false);
                                metrics.onWrite();

                                // Release short term locks.
                                tuple.get2().get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                return new ReplicaResult(true, tuple.get1());
                            });
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        format("Unknown single request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Precesses single request.
     *
     * @param request Single request operation.
     * @param leaseStartTime Lease start time.
     * @return Listener response.
     */
    private CompletableFuture<ReplicaResult> processSingleEntryAction(ReadWriteSingleRowPkReplicaRequest request, long leaseStartTime) {
        UUID txId = request.transactionId();
        BinaryTuple primaryKey = resolvePk(request.primaryKey());
        ReplicationGroupId commitPartitionId = request.commitPartitionId().asReplicationGroupId();

        assert commitPartitionId != null || request.requestType() == RW_GET :
                "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_GET: {
                return resolveRowByPk(primaryKey, txId, (rowId, row, lastCommitTime) -> {
                    if (rowId == null) {
                        metrics.onRead(false);

                        return nullCompletedFuture();
                    }

                    return takeLocksForGet(rowId, txId)
                            .thenCompose(ignored -> validateRwReadAgainstSchemaAfterTakingLocks(txId))
                            .thenApply(ignored -> {
                                metrics.onRead(false);

                                return new ReplicaResult(row, null);
                            });
                });
            }
            case RW_DELETE: {
                return resolveRowByPk(primaryKey, txId, (rowId, row, lastCommitTime) -> {
                    if (rowId == null) {
                        return completedFuture(new ReplicaResult(false, null));
                    }

                    return takeLocksForDelete(row, rowId, txId)
                            .thenCompose(rowLock -> validateWriteAgainstSchemaAfterTakingLocks(request.transactionId()))
                            .thenCompose(catalogVersion -> awaitCleanup(rowId, catalogVersion))
                            .thenCompose(
                                    catalogVersion -> applyUpdateCommand(
                                            request.commitPartitionId().asReplicationGroupId(),
                                            rowId.uuid(),
                                            null,
                                            lastCommitTime,
                                            request.transactionId(),
                                            request.full(),
                                            request.coordinatorId(),
                                            catalogVersion,
                                            request.skipDelayedAck(),
                                            leaseStartTime
                                    )
                            )
                            .thenApply(res -> {
                                metrics.onWrite();

                                return new ReplicaResult(true, res);
                            });
                });
            }
            case RW_GET_AND_DELETE: {
                return resolveRowByPk(primaryKey, txId, (rowId, row, lastCommitTime) -> {
                    if (rowId == null) {
                        metrics.onRead(false);

                        return nullCompletedFuture();
                    }

                    return takeLocksForDelete(row, rowId, txId)
                            .thenCompose(ignored -> validateWriteAgainstSchemaAfterTakingLocks(request.transactionId()))
                            .thenCompose(catalogVersion -> awaitCleanup(rowId, catalogVersion))
                            .thenCompose(
                                    catalogVersion -> applyUpdateCommand(
                                            request.commitPartitionId().asReplicationGroupId(),
                                            rowId.uuid(),
                                            null,
                                            lastCommitTime,
                                            request.transactionId(),
                                            request.full(),
                                            request.coordinatorId(),
                                            catalogVersion,
                                            request.skipDelayedAck(),
                                            leaseStartTime
                                    )
                            )
                            .thenApply(res -> {
                                metrics.onRead(false);
                                metrics.onWrite();

                                return new ReplicaResult(row, res);
                            });
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        format("Unknown single request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Wait for the async cleanup of the provided row to finish.
     *
     * @param rowId Row Ids of existing row that the transaction affects.
     * @param result The value that the returned future will wrap.
     * @param <T> Type of the {@code result}.
     */
    private <T> CompletableFuture<T> awaitCleanup(@Nullable RowId rowId, T result) {
        return (rowId == null ? nullCompletedFuture() : rowCleanupMap.getOrDefault(rowId, nullCompletedFuture()))
                .thenApply(ignored -> result);
    }

    /**
     * Wait for the async cleanup of the provided rows to finish.
     *
     * @param rowIds Row Ids of existing rows that the transaction affects.
     * @param result The value that the returned future will wrap.
     * @param <T> Type of the {@code result}.
     */
    private <T> CompletableFuture<T> awaitCleanup(Collection<RowId> rowIds, T result) {
        if (rowCleanupMap.isEmpty()) {
            return completedFuture(result);
        }

        List<CompletableFuture<?>> list = new ArrayList<>(rowIds.size());

        for (RowId rowId : rowIds) {
            CompletableFuture<?> completableFuture = rowCleanupMap.get(rowId);
            if (completableFuture != null) {
                list.add(completableFuture);
            }
        }
        if (list.isEmpty()) {
            return completedFuture(result);
        }

        return allOf(list.toArray(new CompletableFuture[0]))
                .thenApply(unused -> result);
    }

    /**
     * Extracts a binary tuple corresponding to a part of the row comprised of PK columns.
     *
     * <p>This method must ONLY be invoked when we're sure that a schema version equal to {@code row.schemaVersion()}
     * is already available on this node (see {@link SchemaSyncService}).
     *
     * @param row Row for which to do extraction.
     */
    private BinaryTuple extractPk(BinaryRow row) {
        return pkIndexStorage.get().indexRowResolver().extractColumns(row);
    }

    private BinaryTuple resolvePk(ByteBuffer bytes) {
        return pkIndexStorage.get().resolve(bytes);
    }

    private List<BinaryTuple> resolvePks(List<ByteBuffer> bytesList) {
        var pks = new ArrayList<BinaryTuple>(bytesList.size());

        for (ByteBuffer bytes : bytesList) {
            pks.add(resolvePk(bytes));
        }

        return pks;
    }

    private Cursor<RowId> getFromPkIndex(BinaryTuple key) {
        return pkIndexStorage.get().storage().get(key);
    }

    /**
     * Takes all required locks on a key, before upserting.
     *
     * @param txId Transaction id.
     * @return Future completes with tuple {@link RowId} and collection of {@link Lock}.
     */
    private CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> takeLocksForUpdate(BinaryRow binaryRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableLockKey), LockMode.IX)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.X))
                .thenCompose(ignored -> takePutLockOnIndexes(binaryRow, rowId, txId))
                .thenApply(shortTermLocks -> new IgniteBiTuple<>(rowId, shortTermLocks));
    }

    /**
     * Takes all required locks on a key, before inserting the value.
     *
     * @param binaryRow Table row.
     * @param txId Transaction id.
     * @return Future completes with tuple {@link RowId} and collection of {@link Lock}.
     */
    private CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> takeLocksForInsert(BinaryRow binaryRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableLockKey), LockMode.IX)
                .thenCompose(ignored -> takePutLockOnIndexes(binaryRow, rowId, txId))
                .thenApply(shortTermLocks -> new IgniteBiTuple<>(rowId, shortTermLocks));
    }

    private CompletableFuture<Collection<Lock>> takePutLockOnIndexes(BinaryRow binaryRow, RowId rowId, UUID txId) {
        Collection<IndexLocker> indexes = indexesLockers.get().values();

        if (nullOrEmpty(indexes)) {
            return emptyCollectionCompletedFuture();
        }

        CompletableFuture<Lock>[] locks = new CompletableFuture[indexes.size()];
        int idx = 0;

        for (IndexLocker locker : indexes) {
            locks[idx++] = locker.locksForInsert(txId, binaryRow, rowId);
        }

        return allOf(locks).thenApply(unused -> {
            var shortTermLocks = new ArrayList<Lock>();

            for (CompletableFuture<Lock> lockFut : locks) {
                Lock shortTermLock = lockFut.join();

                if (shortTermLock != null) {
                    shortTermLocks.add(shortTermLock);
                }
            }

            return shortTermLocks;
        });
    }

    private CompletableFuture<?> takeRemoveLockOnIndexes(BinaryRow binaryRow, RowId rowId, UUID txId) {
        Collection<IndexLocker> indexes = indexesLockers.get().values();

        if (nullOrEmpty(indexes)) {
            return nullCompletedFuture();
        }

        CompletableFuture<?>[] locks = new CompletableFuture[indexes.size()];
        int idx = 0;

        for (IndexLocker locker : indexes) {
            locks[idx++] = locker.locksForRemove(txId, binaryRow, rowId);
        }

        return allOf(locks);
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param txId Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for remove.
     */
    private CompletableFuture<RowId> takeLocksForDeleteExact(BinaryRow expectedRow, RowId rowId, BinaryRow actualRow, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableLockKey), LockMode.IX)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.S)) // S lock on RowId
                .thenCompose(ignored -> {
                    if (equalValues(actualRow, expectedRow)) {
                        return lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.X) // X lock on RowId
                                .thenCompose(ignored0 -> takeRemoveLockOnIndexes(actualRow, rowId, txId))
                                .thenApply(exclusiveRowLock -> rowId);
                    }

                    return nullCompletedFuture();
                });
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param txId Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for the key.
     */
    private CompletableFuture<RowId> takeLocksForDelete(BinaryRow binaryRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableLockKey), LockMode.IX)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.X)) // X lock on RowId
                .thenCompose(ignored -> takeRemoveLockOnIndexes(binaryRow, rowId, txId))
                .thenApply(ignored -> rowId);
    }

    /**
     * Takes all required locks on a key, before getting the value.
     *
     * @param txId Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for the key.
     */
    private CompletableFuture<RowId> takeLocksForGet(RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.S) // S lock on RowId
                .thenApply(ignored -> rowId);
    }

    /**
     * Precesses two actions.
     *
     * @param request Two actions operation request.
     * @param leaseStartTime Lease start time.
     * @return Listener response.
     */
    private CompletableFuture<ReplicaResult> processTwoEntriesAction(ReadWriteSwapRowReplicaRequest request, long leaseStartTime) {
        BinaryRow newRow = request.newBinaryRow();
        BinaryRow expectedRow = request.oldBinaryRow();
        ReplicationGroupIdMessage commitPartitionId = request.commitPartitionId();

        assert commitPartitionId != null : "Commit partition is null [type=" + request.requestType() + ']';

        UUID txId = request.transactionId();

        if (request.requestType() == RW_REPLACE) {
            return resolveRowByPk(extractPk(newRow), txId, (rowId, row, lastCommitTime) -> {
                if (rowId == null) {
                    metrics.onRead(false);

                    return completedFuture(new ReplicaResult(false, null));
                }

                return takeLocksForReplace(expectedRow, row, newRow, rowId, txId)
                        .thenCompose(rowIdLock -> {
                            if (rowIdLock == null) {
                                metrics.onRead(false);

                                return completedFuture(new ReplicaResult(false, null));
                            }

                            return validateWriteAgainstSchemaAfterTakingLocks(txId)
                                    .thenCompose(catalogVersion -> awaitCleanup(rowIdLock.get1(), catalogVersion))
                                    .thenCompose(
                                            catalogVersion -> applyUpdateCommand(
                                                    commitPartitionId.asReplicationGroupId(),
                                                    rowIdLock.get1().uuid(),
                                                    newRow,
                                                    lastCommitTime,
                                                    txId,
                                                    request.full(),
                                                    request.coordinatorId(),
                                                    catalogVersion,
                                                    request.skipDelayedAck(),
                                                    leaseStartTime
                                            )
                                    )
                                    .thenApply(res -> new IgniteBiTuple<>(res, rowIdLock))
                                    .thenApply(tuple -> {
                                        metrics.onRead(false);
                                        metrics.onWrite();

                                        // Release short term locks.
                                        tuple.get2().get2()
                                                .forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                        return new ReplicaResult(true, tuple.get1());
                                    });
                        });
            });
        }

        throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                format("Unknown two actions operation [actionType={}]", request.requestType()));
    }

    /**
     * Takes all required locks on a key, before updating the value.
     *
     * @param txId Transaction id.
     * @return Future completes with tuple {@link RowId} and collection of {@link Lock} or {@code null} if there is no suitable row.
     */
    private CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> takeLocksForReplace(BinaryRow expectedRow, @Nullable BinaryRow oldRow,
            BinaryRow newRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableLockKey), LockMode.IX)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.S))
                .thenCompose(ignored -> {
                    if (oldRow != null && equalValues(oldRow, expectedRow)) {
                        return lockManager.acquire(txId, new LockKey(tableLockKey, rowId), LockMode.X) // X lock on RowId
                                .thenCompose(ignored1 -> takePutLockOnIndexes(newRow, rowId, txId))
                                .thenApply(shortTermLocks -> new IgniteBiTuple<>(rowId, shortTermLocks));
                    }

                    return nullCompletedFuture();
                });
    }

    /**
     * Resolves read result to the corresponding binary row. Following rules are used for read result resolution:
     * <ol>
     *     <li>If timestamp is null (RW request), assert that retrieved tx id matches proposed one or that retrieved tx id is null
     *     and return binary row. Currently it's only possible to retrieve write intents if they belong to the same transaction,
     *     locks prevent reading write intents created by others.</li>
     *     <li>If timestamp is not null (RO request), perform write intent resolution if given readResult is a write intent itself
     *     or return binary row otherwise.</li>
     * </ol>
     *
     * @param readResult Read result to resolve.
     * @param txId Nullable transaction id, should be provided if resolution is performed within the context of RW transaction.
     * @param timestamp Timestamp is used in RO transaction only.
     * @param lastCommitted Action to get the latest committed row.
     * @return Future to resolved binary row.
     */
    private CompletableFuture<@Nullable TimedBinaryRow> resolveReadResult(
            ReadResult readResult,
            @Nullable UUID txId,
            @Nullable HybridTimestamp timestamp,
            Supplier<@Nullable TimedBinaryRow> lastCommitted
    ) {
        if (!readResult.isWriteIntent()) {
            return completedFuture(new TimedBinaryRow(readResult.binaryRow(), readResult.commitTimestamp()));
        } else {
            // RW write intent resolution.
            if (timestamp == null) {
                UUID retrievedResultTxId = readResult.transactionId();

                if (txId.equals(retrievedResultTxId)) {
                    // Same transaction - return the retrieved value. It may be either a writeIntent or a regular value.
                    return completedFuture(new TimedBinaryRow(readResult.binaryRow()));
                }
            }

            return resolveWriteIntentAsync(readResult, timestamp, lastCommitted);
        }
    }

    /**
     * Resolves a read result to the matched row. If the result does not match any row, the method returns a future to {@code null}.
     *
     * @param readResult Read result.
     * @param timestamp Timestamp.
     * @param lastCommitted Action to get a last committed row.
     * @return Result future.
     */
    private CompletableFuture<@Nullable TimedBinaryRow> resolveWriteIntentAsync(
            ReadResult readResult,
            @Nullable HybridTimestamp timestamp,
            Supplier<@Nullable TimedBinaryRow> lastCommitted
    ) {
        return inBusyLockAsync(busyLock, () ->
                resolveWriteIntentReadability(readResult, timestamp)
                        .thenApply(writeIntentReadable ->
                                inBusyLock(busyLock, () -> {
                                            if (writeIntentReadable) {
                                                // Even though this readResult is still a write intent entry in the storage
                                                // (therefore it contains txId), we already know it relates to a committed transaction
                                                // and will be cleaned up by an asynchronous task
                                                // started in scheduleTransactionRowAsyncCleanup().
                                                // So it's safe to assume that that this is the latest committed entry.
                                                HybridTimestamp commitTimestamp =
                                                        txManager.stateMeta(readResult.transactionId()).commitTimestamp();

                                                return new TimedBinaryRow(readResult.binaryRow(), commitTimestamp);
                                            }

                                            return lastCommitted.get();
                                        }
                                )
                        )
        );
    }

    /**
     * Schedules an async write intent switch action for the given write intent.
     *
     * @param txId Transaction id.
     * @param rowId Id of a row that we want to clean up.
     * @param meta Resolved transaction state.
     */
    private void scheduleAsyncWriteIntentSwitch(UUID txId, RowId rowId, TransactionMeta meta) {
        TxState txState = meta.txState();

        assert isFinalState(txState) : "Unexpected state [txId=" + txId + ", txState=" + txState + ']';

        HybridTimestamp commitTimestamp = meta.commitTimestamp();

        // Add the resolved row to the set of write intents the transaction created.
        // If the volatile state was lost on restart, we'll have a single item in that set,
        // otherwise the set already contains this value.
        storageUpdateHandler.handleWriteIntentRead(txId, rowId);

        // If the volatile state was lost and we no longer know which rows were affected by this transaction,
        // it is possible that two concurrent RO transactions start resolving write intents for different rows
        // but created by the same transaction.

        // Both normal cleanup and single row cleanup are using txsPendingRowIds map to store write intents.
        // So we don't need a separate method to handle single row case.
        CompletableFuture<?> future = rowCleanupMap.computeIfAbsent(rowId, k -> {
            // The cleanup for this row has already been triggered. For example, we are resolving a write intent for an RW transaction
            // and a concurrent RO transaction resolves the same row, hence computeIfAbsent.

            // We don't need to take the partition snapshots read lock, see #INTERNAL_DOC_PLACEHOLDER why.
            return txManager.executeWriteIntentSwitchAsync(() -> inBusyLock(busyLock,
                    () -> storageUpdateHandler.switchWriteIntents(
                            txId,
                            txState == COMMITTED,
                            commitTimestamp,
                            indexIdsAtRwTxBeginTsOrNull(txId)
                    )
            )).whenComplete((unused, e) -> {
                if (e != null && !ReplicatorRecoverableExceptions.isRecoverable(e)) {
                    LOG.warn("Failed to complete transaction cleanup command [txId=" + txId + ']', e);
                }
            });
        });

        future.handle((v, e) -> rowCleanupMap.remove(rowId, future));
    }

    /**
     * Check whether we can read from the provided write intent.
     *
     * @param writeIntent Write intent to resolve.
     * @param timestamp Timestamp.
     * @return The future completes with {@code true} when the transaction is committed and commit time <= read time, {@code false}
     *         otherwise (whe the transaction is either in progress, or aborted, or committed and commit time > read time).
     */
    private CompletableFuture<Boolean> resolveWriteIntentReadability(ReadResult writeIntent, @Nullable HybridTimestamp timestamp) {
        UUID txId = writeIntent.transactionId();

        return transactionStateResolver.resolveTxState(
                        txId,
                        replicationGroupId(writeIntent.commitTableOrZoneId(), writeIntent.commitPartitionId()),
                        timestamp)
                .thenApply(transactionMeta -> {
                    if (isFinalState(transactionMeta.txState())) {
                        scheduleAsyncWriteIntentSwitch(txId, writeIntent.rowId(), transactionMeta);
                    }

                    return canReadFromWriteIntent(txId, transactionMeta, timestamp);
                });
    }

    private ReplicationGroupId replicationGroupId(int tableOrZoneId, int partitionId) {
        if (nodeProperties.colocationEnabled()) {
            return new ZonePartitionId(tableOrZoneId, partitionId);
        } else {
            return new TablePartitionId(tableOrZoneId, partitionId);
        }
    }

    /**
     * Check whether we can read write intents created by this transaction.
     *
     * @param txId Transaction id.
     * @param txMeta Transaction meta info.
     * @param timestamp Read timestamp.
     * @return {@code true} if we can read from entries created in this transaction (when the transaction was committed and commit time <=
     *         read time).
     */
    private static Boolean canReadFromWriteIntent(UUID txId, TransactionMeta txMeta, @Nullable HybridTimestamp timestamp) {
        assert isFinalState(txMeta.txState()) || txMeta.txState() == PENDING
                : format("Unexpected state defined by write intent resolution [txId={}, txMeta={}].", txId, txMeta);

        if (txMeta.txState() == COMMITTED) {
            boolean readLatest = timestamp == null;

            return readLatest || txMeta.commitTimestamp().compareTo(timestamp) <= 0;
        } else {
            // Either ABORTED or PENDING.
            return false;
        }
    }

    /**
     * Takes current timestamp and makes schema related validations at this timestamp.
     *
     * @param txId Transaction ID.
     * @return Future that will complete when validation completes.
     */
    private CompletableFuture<Void> validateRwReadAgainstSchemaAfterTakingLocks(UUID txId) {
        HybridTimestamp operationTimestamp = clockService.now();

        return schemaSyncService.waitForMetadataCompleteness(operationTimestamp)
                .thenRun(() -> failIfSchemaChangedSinceTxStart(txId, operationTimestamp));
    }

    /**
     * Takes current timestamp and makes schema related validations at this timestamp.
     *
     * @param txId Transaction ID.
     * @return Future that will complete with catalog version associated with given operation though the operation timestamp.
     */
    private CompletableFuture<Integer> validateWriteAgainstSchemaAfterTakingLocks(UUID txId) {
        HybridTimestamp operationTimestamp = clockService.current();

        return reliableCatalogVersionFor(operationTimestamp)
                .thenApply(catalogVersion -> {
                    failIfSchemaChangedSinceTxStart(txId, operationTimestamp);

                    return catalogVersion;
                });
    }

    private UpdateCommand updateCommand(
            ReplicationGroupId commitPartitionId,
            UUID rowUuid,
            @Nullable BinaryRow row,
            @Nullable HybridTimestamp lastCommitTimestamp,
            UUID txId,
            boolean full,
            UUID txCoordinatorId,
            @Nullable HybridTimestamp initiatorTime,
            int catalogVersion,
            @Nullable Long leaseStartTime
    ) {
        UpdateCommandV2Builder bldr = PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                .tableId(tableId)
                .commitPartitionId(replicationGroupIdMessage(commitPartitionId))
                .rowUuid(rowUuid)
                .txId(txId)
                .full(full)
                .initiatorTime(initiatorTime)
                .txCoordinatorId(txCoordinatorId)
                .requiredCatalogVersion(catalogVersion)
                .leaseStartTime(leaseStartTime);

        if (lastCommitTimestamp != null || row != null) {
            TimedBinaryRowMessageBuilder rowMsgBldr = PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage();

            if (lastCommitTimestamp != null) {
                rowMsgBldr.timestamp(lastCommitTimestamp);
            }

            if (row != null) {
                rowMsgBldr.binaryRowMessage(binaryRowMessage(row));
            }

            bldr.messageRowToUpdate(rowMsgBldr.build());
        }

        return bldr.build();
    }

    private static BinaryRowMessage binaryRowMessage(BinaryRow row) {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.binaryRowMessage()
                .binaryTuple(row.tupleSlice())
                .schemaVersion(row.schemaVersion())
                .build();
    }

    private UpdateAllCommand updateAllCommand(
            Map<UUID, TimedBinaryRowMessage> rowsToUpdate,
            ReplicationGroupIdMessage commitPartitionId,
            UUID transactionId,
            HybridTimestamp initiatorTime,
            boolean full,
            UUID txCoordinatorId,
            int catalogVersion,
            @Nullable Long leaseStartTime
    ) {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                .tableId(tableId)
                .commitPartitionId(commitPartitionId)
                .messageRowsToUpdate(rowsToUpdate)
                .txId(transactionId)
                .initiatorTime(initiatorTime)
                .full(full)
                .txCoordinatorId(txCoordinatorId)
                .requiredCatalogVersion(catalogVersion)
                .leaseStartTime(leaseStartTime)
                .build();
    }

    private void failIfSchemaChangedSinceTxStart(UUID txId, HybridTimestamp operationTimestamp) {
        schemaCompatValidator.failIfSchemaChangedAfterTxStart(txId, operationTimestamp, tableId());
    }

    private CompletableFuture<Integer> reliableCatalogVersionFor(HybridTimestamp ts) {
        return reliableCatalogVersions.reliableCatalogVersionFor(ts);
    }

    /**
     * Method to convert from {@link TablePartitionId} object to command-based {@link TablePartitionIdMessage} object.
     *
     * @param tablePartId {@link TablePartitionId} object to convert to {@link TablePartitionIdMessage}.
     * @return {@link TablePartitionIdMessage} object converted from argument.
     */
    public static TablePartitionIdMessage tablePartitionId(TablePartitionId tablePartId) {
        return toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, tablePartId);
    }

    private static ReplicationGroupIdMessage replicationGroupIdMessage(ReplicationGroupId groupId) {
        return toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, groupId);
    }

    /**
     * Class that stores a list of futures for operations that has happened in a specific transaction. Also, the class has a property
     * {@code state} that represents a transaction state.
     */
    private static class TxCleanupReadyFutureList {
        /**
         * Operation type is mapped operation futures.
         */
        final Map<RequestType, Map<OperationId, CompletableFuture<?>>> futures = new EnumMap<>(RequestType.class);
    }

    @Override
    public void onShutdown() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        indexBuildingProcessor.onShutdown();
    }

    private int partId() {
        return replicationGroupId.partitionId();
    }

    private int tableId() {
        return tableId;
    }

    private CompletableFuture<?> processOperationRequestWithTxOperationManagementLogic(
            UUID senderId,
            ReplicaRequest request,
            ReplicaPrimacy replicaPrimacy,
            @Nullable HybridTimestamp opStartTsIfDirectRo
    ) {
        indexBuildingProcessor.incrementRwOperationCountIfNeeded(request);

        UUID txIdLockingLwm = tryToLockLwmIfNeeded(request, opStartTsIfDirectRo);

        try {
            return processOperationRequest(senderId, request, replicaPrimacy, opStartTsIfDirectRo)
                    .whenComplete((unused, throwable) -> {
                        unlockLwmIfNeeded(txIdLockingLwm, request);
                        indexBuildingProcessor.decrementRwOperationCountIfNeeded(request);
                    });
        } catch (Throwable e) {
            try {
                unlockLwmIfNeeded(txIdLockingLwm, request);
            } catch (Throwable unlockProblem) {
                e.addSuppressed(unlockProblem);
            }

            try {
                indexBuildingProcessor.decrementRwOperationCountIfNeeded(request);
            } catch (Throwable decrementProblem) {
                e.addSuppressed(decrementProblem);
            }
            throw e;
        }
    }

    /**
     * Generates a fake transaction ID that will only be used to identify one direct RO operation for purposes of locking and unlocking LWM.
     * It should not be used as a replacement for a real transaction ID in other contexts.
     */
    private static UUID newFakeTxId() {
        return UUID.randomUUID();
    }

    /**
     * For an operation of an RO transaction, attempts to lock LWM on current node (either if the operation is not direct, or if it's direct
     * and concerns more than one key), and does nothing for other types of requests.
     *
     * <p>If lock attempt fails, throws an exception with a specific error code ({@link Transactions#TX_STALE_READ_ONLY_OPERATION_ERR}).
     *
     * <p>For explicit RO transactions, the lock will be later released when cleaning up after the RO transaction had been finished.
     *
     * <p>For direct RO operations (which happen in implicit RO transactions), LWM will be unlocked right after the read had been done
     * (see {@link #unlockLwmIfNeeded(UUID, ReplicaRequest)}).
     *
     * <p>Also, for explicit RO transactions, an automatic unlock is registered on coordinator leave.
     *
     * @param request Request that is being handled.
     * @param opStartTsIfDirectRo Timestamp of operation start if the operation is a direct RO operation, {@code null} otherwise.
     * @return Transaction ID (real for explicit transaction, fake for direct RO operation) that shoiuld be used to lock LWM, or
     *         {@code null} if LWM doesn't need to be locked..
     */
    private @Nullable UUID tryToLockLwmIfNeeded(ReplicaRequest request, @Nullable HybridTimestamp opStartTsIfDirectRo) {
        UUID txIdToLockLwm;
        HybridTimestamp tsToLockLwm = null;

        if (request instanceof ReadOnlyDirectMultiRowReplicaRequest
                && ((ReadOnlyDirectMultiRowReplicaRequest) request).primaryKeys().size() > 1) {
            assert opStartTsIfDirectRo != null;

            txIdToLockLwm = newFakeTxId();
            tsToLockLwm = opStartTsIfDirectRo;
        } else if (request instanceof ReadOnlyReplicaRequest) {
            ReadOnlyReplicaRequest readOnlyRequest = (ReadOnlyReplicaRequest) request;
            txIdToLockLwm = readOnlyRequest.transactionId();
            tsToLockLwm = readOnlyRequest.readTimestamp();
        } else {
            txIdToLockLwm = null;
        }

        if (txIdToLockLwm != null) {
            if (!lowWatermark.tryLock(txIdToLockLwm, tsToLockLwm)) {
                throw new TransactionException(Transactions.TX_STALE_READ_ONLY_OPERATION_ERR, "Read timestamp is not available anymore.");
            }

            registerAutoLwmUnlockOnCoordinatorLeaveIfNeeded(request, txIdToLockLwm);
        }

        return txIdToLockLwm;
    }

    private void registerAutoLwmUnlockOnCoordinatorLeaveIfNeeded(ReplicaRequest request, UUID txIdToLockLwm) {
        if (request instanceof ReadOnlyReplicaRequest) {
            ReadOnlyReplicaRequest readOnlyReplicaRequest = (ReadOnlyReplicaRequest) request;

            UUID coordinatorId = readOnlyReplicaRequest.coordinatorId();
            // TODO: remove null check after IGNITE-24120 is sorted out.
            if (coordinatorId != null) {
                FullyQualifiedResourceId resourceId = new FullyQualifiedResourceId(txIdToLockLwm, txIdToLockLwm);
                remotelyTriggeredResourceRegistry.register(resourceId, coordinatorId, () -> () -> lowWatermark.unlock(txIdToLockLwm));
            }
        }
    }

    private void unlockLwmIfNeeded(@Nullable UUID txIdToUnlockLwm, ReplicaRequest request) {
        if (txIdToUnlockLwm != null && request instanceof ReadOnlyDirectReplicaRequest) {
            lowWatermark.unlock(txIdToUnlockLwm);
        }
    }

    private List<Integer> indexIdsAtRwTxBeginTs(UUID txId) {
        return TableUtils.indexIdsAtRwTxBeginTs(catalogService, txId, tableId());
    }

    private @Nullable List<Integer> indexIdsAtRwTxBeginTsOrNull(UUID txId) {
        return TableUtils.indexIdsAtRwTxBeginTsOrNull(catalogService, txId, tableId());
    }

    private int tableVersionByTs(HybridTimestamp ts) {
        Catalog catalog = catalogService.activeCatalog(ts.longValue());

        CatalogTableDescriptor table = catalog.table(tableId());

        assert table != null : "tableId=" + tableId() + ", catalogVersion=" + catalog.version();

        return table.latestSchemaVersion();
    }

    private static @Nullable BinaryRow binaryRow(@Nullable TimedBinaryRow timedBinaryRow) {
        return timedBinaryRow == null ? null : timedBinaryRow.binaryRow();
    }

    private @Nullable BinaryRow upgrade(@Nullable BinaryRow source, int targetSchemaVersion) {
        return source == null ? null : new BinaryRowUpgrader(schemaRegistry, targetSchemaVersion).upgrade(source);
    }

    @TestOnly
    public void cleanupLocally(UUID txId, boolean commit, @Nullable HybridTimestamp commitTimestamp) {
        storageUpdateHandler.switchWriteIntents(txId, commit, commitTimestamp, null);
    }

    /**
     * Operation unique identifier.
     */
    private static class OperationId {
        /** Operation node initiator id. */
        private UUID initiatorId;

        /** Timestamp. */
        private long ts;

        /**
         * The constructor.
         *
         * @param initiatorId Sender node id.
         * @param ts Timestamp.
         */
        public OperationId(UUID initiatorId, long ts) {
            this.initiatorId = initiatorId;
            this.ts = ts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            OperationId that = (OperationId) o;

            if (ts != that.ts) {
                return false;
            }
            return initiatorId.equals(that.initiatorId);
        }

        @Override
        public int hashCode() {
            int result = initiatorId.hashCode();
            result = 31 * result + (int) (ts ^ (ts >>> 32));
            return result;
        }
    }
}
