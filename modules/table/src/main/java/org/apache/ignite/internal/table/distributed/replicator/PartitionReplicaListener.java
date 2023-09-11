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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toIndexDescriptor;
import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toTableDescriptor;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationUtils.findTableView;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.filter;
import static org.apache.ignite.internal.util.IgniteUtils.findAny;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.exception.UnsupportedReplicaRequestException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.BinaryTupleComparator;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.SortedIndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommandBuilder;
import org.apache.ignite.internal.table.distributed.command.TablePartitionIdMessage;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommandBuilder;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryRowMessage;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryTupleMessage;
import org.apache.ignite.internal.table.distributed.replication.request.CommittableTxRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanCloseReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSwapRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.Schemas;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.TxCleanupReplicaRequest;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.CursorUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/** Partition replication listener. */
public class PartitionReplicaListener implements ReplicaListener {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionReplicaListener.class);

    /** Factory to create RAFT command messages. */
    private static final TableMessagesFactory MSG_FACTORY = new TableMessagesFactory();

    /** Factory for creating replica command messages. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Replication group id. */
    private final TablePartitionId replicationGroupId;

    /** Primary key index. */
    private final Lazy<TableSchemaAwareIndexStorage> pkIndexStorage;

    /** Secondary indices. */
    private final Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages;

    /** Versioned partition storage. */
    private final MvPartitionStorage mvDataStorage;

    /** Raft client. */
    private final RaftGroupService raftClient;

    /** Tx manager. */
    private final TxManager txManager;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Handler that processes updates writing them to storage. */
    private final StorageUpdateHandler storageUpdateHandler;

    /**
     * Cursors map. The key of the map is internal Ignite uuid which consists of a transaction id ({@link UUID}) and a cursor id
     * ({@link Long}).
     */
    private final ConcurrentNavigableMap<IgniteUuid, Cursor<?>> cursors;

    /** Tx state storage. */
    private final TxStateStorage txStateStorage;

    /** Hybrid clock. */
    private final HybridClock hybridClock;

    /** Safe time. */
    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTime;

    /** Placement Driver. */
    private final PlacementDriver placementDriver;

    /** Runs async scan tasks for effective tail recursion execution (avoid deep recursive calls). */
    private final Executor scanRequestExecutor;

    /**
     * Map to control clock's update in the read only transactions concurrently with a commit timestamp.
     * TODO: IGNITE-20034 review this after the commit timestamp will be provided from a commit request (request.commitTimestamp()).
     */
    private final ConcurrentHashMap<UUID, CompletableFuture<TxMeta>> txTimestampUpdateMap = new ConcurrentHashMap<>();

    private final Supplier<Map<Integer, IndexLocker>> indexesLockers;

    private final ConcurrentMap<UUID, TxCleanupReadyFutureList> txCleanupReadyFutures = new ConcurrentHashMap<>();

    private final SchemaCompatValidator schemaCompatValidator;

    /** Instance of the local node. */
    private final ClusterNode localNode;

    /** Table storage. */
    private final MvTableStorage mvTableStorage;

    /** Index builder. */
    private final IndexBuilder indexBuilder;

    private final SchemaSyncService schemaSyncService;

    private final CatalogService catalogService;

    /** Listener for configuration indexes, {@code null} if the replica is not the leader. */
    private final AtomicReference<ConfigurationNamedListListener<TableIndexView>> indexesConfigurationListener = new AtomicReference<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Flag indicates whether the current replica is the primary. */
    private volatile boolean primary;

    private final TablesConfiguration tablesConfig;

    private final int pkLength;

    /**
     * The constructor.
     *
     * @param mvDataStorage Data storage.
     * @param raftClient Raft client.
     * @param txManager Transaction manager.
     * @param lockManager Lock manager.
     * @param partId Partition id.
     * @param tableId Table id.
     * @param indexesLockers Index lock helper objects.
     * @param pkIndexStorage Pk index storage.
     * @param secondaryIndexStorages Secondary index storages.
     * @param hybridClock Hybrid clock.
     * @param safeTime Safe time clock.
     * @param txStateStorage Transaction state storage.
     * @param placementDriver Placement driver.
     * @param storageUpdateHandler Handler that processes updates writing them to storage.
     * @param localNode Instance of the local node.
     * @param mvTableStorage Table storage.
     * @param indexBuilder Index builder.
     * @param tablesConfig Tables configuration.
     */
    public PartitionReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            TxManager txManager,
            LockManager lockManager,
            Executor scanRequestExecutor,
            int partId,
            int tableId,
            Supplier<Map<Integer, IndexLocker>> indexesLockers,
            Lazy<TableSchemaAwareIndexStorage> pkIndexStorage,
            Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages,
            HybridClock hybridClock,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
            TxStateStorage txStateStorage,
            PlacementDriver placementDriver,
            StorageUpdateHandler storageUpdateHandler,
            Schemas schemas,
            ClusterNode localNode,
            MvTableStorage mvTableStorage,
            IndexBuilder indexBuilder,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            TablesConfiguration tablesConfig
    ) {
        this.mvDataStorage = mvDataStorage;
        this.raftClient = raftClient;
        this.txManager = txManager;
        this.lockManager = lockManager;
        this.scanRequestExecutor = scanRequestExecutor;
        this.indexesLockers = indexesLockers;
        this.pkIndexStorage = pkIndexStorage;
        this.secondaryIndexStorages = secondaryIndexStorages;
        this.hybridClock = hybridClock;
        this.safeTime = safeTime;
        this.txStateStorage = txStateStorage;
        this.placementDriver = placementDriver;
        this.storageUpdateHandler = storageUpdateHandler;
        this.localNode = localNode;
        this.mvTableStorage = mvTableStorage;
        this.indexBuilder = indexBuilder;
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
        this.tablesConfig = tablesConfig;

        this.replicationGroupId = new TablePartitionId(tableId, partId);

        cursors = new ConcurrentSkipListMap<>(IgniteUuid.globalOrderComparator());

        schemaCompatValidator = new SchemaCompatValidator(schemas);

        TableView tableConfig = findTableView(tablesConfig.tables().value(), tableId);

        assert tableConfig != null;

        pkLength = tableConfig.primaryKey().columns().length;
    }

    @Override
    public CompletableFuture<?> invoke(ReplicaRequest request, String senderId) {
        if (request instanceof TxStateReplicaRequest) {
            return processTxStateReplicaRequest((TxStateReplicaRequest) request);
        }

        return ensureReplicaIsPrimary(request).thenCompose(isPrimary -> processRequest(request, isPrimary, senderId));
    }

    private CompletableFuture<?> processRequest(ReplicaRequest request, @Nullable Boolean isPrimary, String senderId) {
        if (request instanceof CommittableTxRequest) {
            var req = (CommittableTxRequest) request;

            // Saving state is not needed for full transactions.
            if (!req.full()) {
                txManager.updateTxMeta(req.transactionId(), old -> new TxStateMeta(PENDING, senderId, null));
            }
        }

        if (request instanceof ReadWriteSingleRowReplicaRequest) {
            var req = (ReadWriteSingleRowReplicaRequest) request;

            return appendTxCommand(req.transactionId(), req.requestType(), req.full(), () -> processSingleEntryAction(req, senderId));
        } else if (request instanceof ReadWriteSingleRowPkReplicaRequest) {
            var req = (ReadWriteSingleRowPkReplicaRequest) request;

            return appendTxCommand(req.transactionId(), req.requestType(), req.full(), () -> processSingleEntryAction(req, senderId));
        } else if (request instanceof ReadWriteMultiRowReplicaRequest) {
            var req = (ReadWriteMultiRowReplicaRequest) request;

            return appendTxCommand(req.transactionId(), req.requestType(), req.full(), () -> processMultiEntryAction(req, senderId));
        } else if (request instanceof ReadWriteMultiRowPkReplicaRequest) {
            var req = (ReadWriteMultiRowPkReplicaRequest) request;

            return appendTxCommand(req.transactionId(), req.requestType(), req.full(), () -> processMultiEntryAction(req, senderId));
        } else if (request instanceof ReadWriteSwapRowReplicaRequest) {
            var req = (ReadWriteSwapRowReplicaRequest) request;

            return appendTxCommand(req.transactionId(), req.requestType(), req.full(), () -> processTwoEntriesAction(req, senderId));
        } else if (request instanceof ReadWriteScanRetrieveBatchReplicaRequest) {
            var req = (ReadWriteScanRetrieveBatchReplicaRequest) request;

            // Implicit RW scan can be committed locally on a last batch or error.
            return appendTxCommand(req.transactionId(), RequestType.RW_SCAN, false, () -> processScanRetrieveBatchAction(req, senderId))
                    .handle((rows, err) -> {
                        if (req.full() && (err != null || rows.size() < req.batchSize())) {
                            releaseTxLocks(req.transactionId());
                        }

                        if (err != null) {
                            ExceptionUtils.sneakyThrow(err);
                        }

                        return rows;
                    });
        } else if (request instanceof ReadWriteScanCloseReplicaRequest) {
            processScanCloseAction((ReadWriteScanCloseReplicaRequest) request);

            return completedFuture(null);
        } else if (request instanceof TxFinishReplicaRequest) {
            return processTxFinishAction((TxFinishReplicaRequest) request, senderId);
        } else if (request instanceof TxCleanupReplicaRequest) {
            return processTxCleanupAction((TxCleanupReplicaRequest) request);
        } else if (request instanceof ReadOnlySingleRowPkReplicaRequest) {
            return processReadOnlySingleEntryAction((ReadOnlySingleRowPkReplicaRequest) request, isPrimary);
        } else if (request instanceof ReadOnlyMultiRowPkReplicaRequest) {
            return processReadOnlyMultiEntryAction((ReadOnlyMultiRowPkReplicaRequest) request, isPrimary);
        } else if (request instanceof ReadOnlyScanRetrieveBatchReplicaRequest) {
            return processReadOnlyScanRetrieveBatchAction((ReadOnlyScanRetrieveBatchReplicaRequest) request, isPrimary);
        } else if (request instanceof ReplicaSafeTimeSyncRequest) {
            return processReplicaSafeTimeSyncRequest((ReplicaSafeTimeSyncRequest) request, isPrimary);
        } else {
            throw new UnsupportedReplicaRequestException(request.getClass());
        }
    }

    /**
     * Processes a transaction state request.
     *
     * @param request Transaction state request.
     * @return Result future.
     */
    private CompletableFuture<LeaderOrTxState> processTxStateReplicaRequest(TxStateReplicaRequest request) {
        return raftClient.refreshAndGetLeaderWithTerm()
                .thenCompose(replicaAndTerm -> {
                    Peer leader = replicaAndTerm.leader();

                    if (isLocalPeer(leader)) {
                        CompletableFuture<TxMeta> txStateFut = getTxStateConcurrently(request);

                        return txStateFut.thenApply(txMeta -> new LeaderOrTxState(null, txMeta));
                    } else {
                        return completedFuture(new LeaderOrTxState(leader.consistentId(), null));
                    }
                });
    }

    /**
     * Gets a transaction state or {@code null}, if the transaction is not completed.
     *
     * @param txStateReq Transaction state request.
     * @return Future to transaction state meta or {@code null}.
     */
    private CompletableFuture<TxMeta> getTxStateConcurrently(TxStateReplicaRequest txStateReq) {
        //TODO: IGNITE-20034 review this after the commit timestamp will be provided from a commit request (request.commitTimestamp()).
        CompletableFuture<TxMeta> txStateFut = new CompletableFuture<>();

        txTimestampUpdateMap.compute(txStateReq.txId(), (uuid, fut) -> {
            if (fut != null) {
                fut.thenAccept(txStateFut::complete);
            } else {
                TxMeta txMeta = txStateStorage.get(txStateReq.txId());

                if (txMeta == null) {
                    // All future transactions will be committed after the resolution processed.
                    hybridClock.update(txStateReq.readTimestamp());
                }

                txStateFut.complete(txMeta);
            }

            return null;
        });

        return txStateFut;
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
            Boolean isPrimary
    ) {
        requireNonNull(isPrimary);

        UUID txId = request.transactionId();
        int batchCount = request.batchSize();
        HybridTimestamp readTimestamp = request.readTimestamp();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        CompletableFuture<Void> safeReadFuture = isPrimaryInTimestamp(isPrimary, readTimestamp) ? completedFuture(null)
                : safeTime.waitFor(readTimestamp);

        if (request.indexToUse() != null) {
            TableSchemaAwareIndexStorage indexStorage = secondaryIndexStorages.get().get(request.indexToUse());

            if (indexStorage == null) {
                throw new AssertionError("Index not found: uuid=" + request.indexToUse());
            }

            if (request.exactKey() != null) {
                assert request.lowerBoundPrefix() == null && request.upperBoundPrefix() == null : "Index lookup doesn't allow bounds.";

                return safeReadFuture.thenCompose(unused -> lookupIndex(request, indexStorage));
            }

            assert indexStorage.storage() instanceof SortedIndexStorage;

            return safeReadFuture.thenCompose(unused -> scanSortedIndex(request, indexStorage));
        }

        return safeReadFuture.thenCompose(unused -> retrieveExactEntriesUntilCursorEmpty(txId, readTimestamp, cursorId, batchCount));
    }

    /**
     * Extracts exact amount of entries, or less if cursor is become empty, from a cursor on the specific time.
     *
     * @param txId Transaction id is used for RW only.
     * @param readTimestamp Timestamp of the moment when that moment when the data will be extracted.
     * @param cursorId Cursor id.
     * @param count Amount of entries which sill be extracted.
     * @return Result future.
     */
    private CompletableFuture<List<BinaryRow>> retrieveExactEntriesUntilCursorEmpty(
            @Nullable UUID txId,
            @Nullable HybridTimestamp readTimestamp,
            IgniteUuid cursorId,
            int count
    ) {
        @SuppressWarnings("resource") PartitionTimestampCursor cursor = (PartitionTimestampCursor) cursors.computeIfAbsent(cursorId,
                id -> mvDataStorage.scan(readTimestamp == null ? HybridTimestamp.MAX_VALUE : readTimestamp));

        var resolutionFuts = new ArrayList<CompletableFuture<BinaryRow>>(count);

        while (resolutionFuts.size() < count && cursor.hasNext()) {
            ReadResult readResult = cursor.next();
            HybridTimestamp newestCommitTimestamp = readResult.newestCommitTimestamp();

            BinaryRow candidate =
                    newestCommitTimestamp == null || !readResult.isWriteIntent() ? null : cursor.committed(newestCommitTimestamp);

            resolutionFuts.add(resolveReadResult(readResult, txId, readTimestamp, () -> candidate));
        }

        return allOf(resolutionFuts.toArray(new CompletableFuture[0])).thenCompose(unused -> {
            var rows = new ArrayList<BinaryRow>(count);

            for (CompletableFuture<BinaryRow> resolutionFut : resolutionFuts) {
                BinaryRow resolvedReadResult = resolutionFut.join();

                if (resolvedReadResult != null) {
                    rows.add(resolvedReadResult);
                }
            }

            if (rows.size() < count && cursor.hasNext()) {
                return retrieveExactEntriesUntilCursorEmpty(txId, readTimestamp, cursorId, count - rows.size()).thenApply(binaryRows -> {
                    rows.addAll(binaryRows);

                    return rows;
                });
            } else {
                return completedFuture(rows);
            }
        });
    }

    /**
     * Extracts exact amount of entries, or less if cursor is become empty, from a cursor on the specific time. Use it for RW.
     *
     * @param txId Transaction id.
     * @param cursorId Cursor id.
     * @return Future finishes with the resolved binary row.
     */
    private CompletableFuture<List<BinaryRow>> retrieveExactEntriesUntilCursorEmpty(UUID txId, IgniteUuid cursorId, int count) {
        return retrieveExactEntriesUntilCursorEmpty(txId, null, cursorId, count).thenCompose(rows -> {
            if (nullOrEmpty(rows)) {
                return completedFuture(Collections.emptyList());
            }

            CompletableFuture<?>[] futs = new CompletableFuture[rows.size()];

            for (int i = 0; i < rows.size(); i++) {
                BinaryRow row = rows.get(i);

                futs[i] = schemaCompatValidator.validateBackwards(row.schemaVersion(), tableId(), txId)
                        .thenCompose(validationResult -> {
                            if (validationResult.isSuccessful()) {
                                return completedFuture(row);
                            } else {
                                throw new IncompatibleSchemaException("Operation failed because schema "
                                        + validationResult.fromSchemaVersion() + " is not backward-compatible with "
                                        + validationResult.toSchemaVersion() + " for table " + validationResult.failedTableId());
                            }
                        });
            }

            return allOf(futs).thenApply((unused) -> rows);
        });
    }

    /**
     * Processes single entry request for read only transaction.
     *
     * @param request Read only single entry request.
     * @param isPrimary Whether the given replica is primary.
     * @return Result future.
     */
    private CompletableFuture<BinaryRow> processReadOnlySingleEntryAction(ReadOnlySingleRowPkReplicaRequest request, Boolean isPrimary) {
        BinaryTuple primaryKey = resolvePk(request.primaryKey());
        HybridTimestamp readTimestamp = request.readTimestamp();

        if (request.requestType() != RequestType.RO_GET) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        CompletableFuture<Void> safeReadFuture = isPrimaryInTimestamp(isPrimary, readTimestamp) ? completedFuture(null)
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
    private boolean isPrimaryInTimestamp(Boolean isPrimary, HybridTimestamp timestamp) {
        return isPrimary && hybridClock.now().compareTo(timestamp) > 0;
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
            Boolean isPrimary
    ) {
        List<BinaryTuple> primaryKeys = resolvePks(request.primaryKeys());
        HybridTimestamp readTimestamp = request.readTimestamp();

        if (request.requestType() != RequestType.RO_GET_ALL) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    format("Unknown single request [actionType={}]", request.requestType()));
        }

        CompletableFuture<Void> safeReadFuture = isPrimaryInTimestamp(isPrimary, readTimestamp) ? completedFuture(null)
                : safeTime.waitFor(request.readTimestamp());

        return safeReadFuture.thenCompose(unused -> {
            CompletableFuture<BinaryRow>[] resolutionFuts = new CompletableFuture[primaryKeys.size()];

            for (int i = 0; i < primaryKeys.size(); i++) {
                resolutionFuts[i] = resolveRowByPkForReadOnly(primaryKeys.get(i), readTimestamp);
            }

            return allOf(resolutionFuts).thenApply(unused1 -> {
                var result = new ArrayList<BinaryRow>(resolutionFuts.length);

                for (CompletableFuture<BinaryRow> resolutionFut : resolutionFuts) {
                    BinaryRow resolvedReadResult = resolutionFut.join();

                    result.add(resolvedReadResult);
                }

                return result;
            });
        });
    }

    /**
     * Handler to process {@link ReplicaSafeTimeSyncRequest}.
     *
     * @param request Request.
     * @param isPrimary Whether is primary replica.
     * @return Future.
     */
    private CompletableFuture<Void> processReplicaSafeTimeSyncRequest(ReplicaSafeTimeSyncRequest request, Boolean isPrimary) {
        requireNonNull(isPrimary);

        if (!isPrimary) {
            return completedFuture(null);
        }

        return raftClient.run(REPLICA_MESSAGES_FACTORY.safeTimeSyncCommand().safeTimeLong(hybridClock.nowLong()).build());
    }

    /**
     * Close all cursors connected with a transaction.
     *
     * @param txId Transaction id.
     * @throws Exception When an issue happens on cursor closing.
     */
    private void closeAllTransactionCursors(UUID txId) {
        var lowCursorId = new IgniteUuid(txId, Long.MIN_VALUE);
        var upperCursorId = new IgniteUuid(txId, Long.MAX_VALUE);

        Map<IgniteUuid, ? extends Cursor<?>> txCursors = cursors.subMap(lowCursorId, true, upperCursorId, true);

        ReplicationException ex = null;

        for (AutoCloseable cursor : txCursors.values()) {
            try {
                cursor.close();
            } catch (Exception e) {
                if (ex == null) {
                    ex = new ReplicationException(Replicator.REPLICA_COMMON_ERR,
                            format("Close cursor exception [replicaGrpId={}, msg={}]", replicationGroupId,
                                    e.getMessage()), e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        txCursors.clear();

        if (ex != null) {
            throw ex;
        }
    }

    /**
     * Processes scan close request.
     *
     * @param request Scan close request operation.
     */
    private void processScanCloseAction(ReadWriteScanCloseReplicaRequest request) {
        UUID txId = request.transactionId();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        Cursor<?> cursor = cursors.remove(cursorId);

        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception e) {
                throw new ReplicationException(Replicator.REPLICA_COMMON_ERR,
                        format("Close cursor exception [replicaGrpId={}, msg={}]", replicationGroupId,
                                e.getMessage()), e);
            }
        }
    }

    /**
     * Processes scan retrieve batch request.
     *
     * @param request Scan retrieve batch request operation.
     * @return Listener response.
     */
    private CompletableFuture<List<BinaryRow>> processScanRetrieveBatchAction(
            ReadWriteScanRetrieveBatchReplicaRequest request,
            String txCoordinatorId
    ) {
        if (request.indexToUse() != null) {
            TableSchemaAwareIndexStorage indexStorage = secondaryIndexStorages.get().get(request.indexToUse());

            if (indexStorage == null) {
                throw new AssertionError("Index not found: uuid=" + request.indexToUse());
            }

            if (request.exactKey() != null) {
                assert request.lowerBoundPrefix() == null && request.upperBoundPrefix() == null : "Index lookup doesn't allow bounds.";

                return lookupIndex(request, indexStorage.storage());
            }

            assert indexStorage.storage() instanceof SortedIndexStorage;

            return scanSortedIndex(request, indexStorage);
        }

        UUID txId = request.transactionId();
        int batchCount = request.batchSize();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        return lockManager.acquire(txId, new LockKey(tableId()), LockMode.S).thenCompose(tblLock ->
                retrieveExactEntriesUntilCursorEmpty(txId, cursorId, batchCount));
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

        int batchCount = request.batchSize();
        HybridTimestamp timestamp = request.readTimestamp();

        IgniteUuid cursorId = new IgniteUuid(request.transactionId(), request.scanId());

        BinaryTuple key = request.exactKey().asBinaryTuple();

        Cursor<RowId> cursor = (Cursor<RowId>) cursors.computeIfAbsent(cursorId,
                id -> indexStorage.get(key));

        var result = new ArrayList<BinaryRow>(batchCount);

        Cursor<IndexRow> indexRowCursor = CursorUtils.map(cursor, rowId -> new IndexRowImpl(key, rowId));

        return continueReadOnlyIndexScan(schemaAwareIndexStorage, indexRowCursor, timestamp, batchCount, result)
                .thenCompose(ignore -> completedFuture(result));
    }

    private CompletableFuture<List<BinaryRow>> lookupIndex(
            ReadWriteScanRetrieveBatchReplicaRequest request,
            IndexStorage indexStorage
    ) {
        UUID txId = request.transactionId();
        int batchCount = request.batchSize();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        Integer indexId = request.indexToUse();

        BinaryTuple exactKey = request.exactKey().asBinaryTuple();

        return lockManager.acquire(txId, new LockKey(indexId), LockMode.IS).thenCompose(idxLock -> { // Index IS lock
            return lockManager.acquire(txId, new LockKey(tableId()), LockMode.IS).thenCompose(tblLock -> { // Table IS lock
                return lockManager.acquire(txId, new LockKey(indexId, exactKey.byteBuffer()), LockMode.S)
                        .thenCompose(indRowLock -> { // Hash index bucket S lock
                            Cursor<RowId> cursor = (Cursor<RowId>) cursors.computeIfAbsent(cursorId, id -> indexStorage.get(exactKey));

                            var result = new ArrayList<BinaryRow>(batchCount);

                            return continueIndexLookup(txId, cursor, batchCount, result)
                                    .thenApply(ignore -> result);
                        });
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
        int batchCount = request.batchSize();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        Integer indexId = request.indexToUse();

        BinaryTupleMessage lowerBoundMessage = request.lowerBoundPrefix();
        BinaryTupleMessage upperBoundMessage = request.upperBoundPrefix();

        BinaryTuplePrefix lowerBound = lowerBoundMessage == null ? null : lowerBoundMessage.asBinaryTuplePrefix();
        BinaryTuplePrefix upperBound = upperBoundMessage == null ? null : upperBoundMessage.asBinaryTuplePrefix();

        int flags = request.flags();

        return lockManager.acquire(txId, new LockKey(indexId), LockMode.IS).thenCompose(idxLock -> { // Index IS lock
            return lockManager.acquire(txId, new LockKey(tableId()), LockMode.IS).thenCompose(tblLock -> { // Table IS lock
                var comparator = new BinaryTupleComparator(indexStorage.indexDescriptor().columns());

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

                Cursor<IndexRow> cursor = (Cursor<IndexRow>) cursors.computeIfAbsent(cursorId,
                        id -> indexStorage.scan(
                                lowerBound,
                                // We have to handle upperBound on a level of replication listener,
                                // for correctness of taking of a range lock.
                                null,
                                flags
                        ));

                SortedIndexLocker indexLocker = (SortedIndexLocker) indexesLockers.get().get(indexId);

                var result = new ArrayList<BinaryRow>(batchCount);

                return continueIndexScan(txId, schemaAwareIndexStorage, indexLocker, cursor, batchCount, result, isUpperBoundAchieved)
                        .thenApply(ignore -> result);
            });
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

        UUID txId = request.transactionId();
        int batchCount = request.batchSize();
        HybridTimestamp timestamp = request.readTimestamp();

        IgniteUuid cursorId = new IgniteUuid(txId, request.scanId());

        BinaryTupleMessage lowerBoundMessage = request.lowerBoundPrefix();
        BinaryTupleMessage upperBoundMessage = request.upperBoundPrefix();

        BinaryTuplePrefix lowerBound = lowerBoundMessage == null ? null : lowerBoundMessage.asBinaryTuplePrefix();
        BinaryTuplePrefix upperBound = upperBoundMessage == null ? null : upperBoundMessage.asBinaryTuplePrefix();

        int flags = request.flags();

        Cursor<IndexRow> cursor = (Cursor<IndexRow>) cursors.computeIfAbsent(cursorId,
                id -> indexStorage.scan(
                        lowerBound,
                        upperBound,
                        flags
                ));

        var result = new ArrayList<BinaryRow>(batchCount);

        return continueReadOnlyIndexScan(schemaAwareIndexStorage, cursor, timestamp, batchCount, result)
                .thenApply(ignore -> result);
    }

    private CompletableFuture<Void> continueReadOnlyIndexScan(
            TableSchemaAwareIndexStorage schemaAwareIndexStorage,
            Cursor<IndexRow> cursor,
            HybridTimestamp timestamp,
            int batchSize,
            List<BinaryRow> result
    ) {
        if (result.size() >= batchSize || !cursor.hasNext()) {
            return completedFuture(null);
        }

        IndexRow indexRow = cursor.next();

        RowId rowId = indexRow.rowId();

        return resolvePlainReadResult(rowId, null, timestamp).thenComposeAsync(resolvedReadResult -> {
            if (resolvedReadResult != null && indexRowMatches(indexRow, resolvedReadResult, schemaAwareIndexStorage)) {
                result.add(resolvedReadResult);
            }

            return continueReadOnlyIndexScan(schemaAwareIndexStorage, cursor, timestamp, batchSize, result);
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
     * @return Future.
     */
    private CompletableFuture<Void> continueIndexScan(
            UUID txId,
            TableSchemaAwareIndexStorage schemaAwareIndexStorage,
            SortedIndexLocker indexLocker,
            Cursor<IndexRow> indexCursor,
            int batchSize,
            List<BinaryRow> result,
            Predicate<IndexRow> isUpperBoundAchieved
    ) {
        if (result.size() == batchSize) { // Batch is full, exit loop.
            return completedFuture(null);
        }

        return indexLocker.locksForScan(txId, indexCursor)
                .thenCompose(currentRow -> { // Index row S lock
                    if (isUpperBoundAchieved.test(currentRow)) {
                        return completedFuture(null); // End of range reached. Exit loop.
                    }

                    RowId rowId = currentRow.rowId();

                    return lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.S)
                            .thenComposeAsync(rowLock -> { // Table row S lock
                                return resolvePlainReadResult(rowId, txId).thenCompose(resolvedReadResult -> {
                                    if (resolvedReadResult != null) {
                                        if (indexRowMatches(currentRow, resolvedReadResult, schemaAwareIndexStorage)) {
                                            result.add(resolvedReadResult);
                                        }
                                    }

                                    // Proceed scan.
                                    return continueIndexScan(
                                            txId,
                                            schemaAwareIndexStorage,
                                            indexLocker,
                                            indexCursor,
                                            batchSize,
                                            result,
                                            isUpperBoundAchieved
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
            return completedFuture(null);
        }

        RowId rowId = indexCursor.next();

        return lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.S)
                .thenComposeAsync(rowLock -> { // Table row S lock
                    return resolvePlainReadResult(rowId, txId).thenCompose(resolvedReadResult -> {
                        if (resolvedReadResult != null) {
                            result.add(resolvedReadResult);
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
    private CompletableFuture<BinaryRow> resolvePlainReadResult(RowId rowId, @Nullable UUID txId, @Nullable HybridTimestamp timestamp) {
        ReadResult readResult = mvDataStorage.read(rowId, timestamp == null ? HybridTimestamp.MAX_VALUE : timestamp);

        return resolveReadResult(readResult, txId, timestamp, () -> {
            if (readResult.newestCommitTimestamp() == null) {
                return null;
            }

            ReadResult committedReadResult = mvDataStorage.read(rowId, readResult.newestCommitTimestamp());

            assert !committedReadResult.isWriteIntent() :
                    "The result is not committed [rowId=" + rowId + ", timestamp="
                            + readResult.newestCommitTimestamp() + ']';

            return committedReadResult.binaryRow();
        });
    }

    /**
     * Resolves a result received from a direct storage read. Use it for RW.
     *
     * @param rowId Row id.
     * @param txId Transaction id.
     * @return Future finishes with the resolved binary row.
     */
    private CompletableFuture<BinaryRow> resolvePlainReadResult(RowId rowId, UUID txId) {
        return resolvePlainReadResult(rowId, txId, null).thenCompose(row -> {
            if (row == null) {
                return completedFuture(null);
            }

            return schemaCompatValidator.validateBackwards(row.schemaVersion(), tableId(), txId)
                    .thenApply(validationResult -> {
                        if (validationResult.isSuccessful()) {
                            return row;
                        } else {
                            throw new IncompatibleSchemaException("Operation failed because schema "
                                    + validationResult.fromSchemaVersion() + " is not backward-compatible with "
                                    + validationResult.toSchemaVersion() + " for table " + validationResult.failedTableId());
                        }
                    });
        });
    }

    private CompletableFuture<Void> continueReadOnlyIndexLookup(
            Cursor<RowId> indexCursor,
            HybridTimestamp timestamp,
            int batchSize,
            List<BinaryRow> result
    ) {
        if (result.size() >= batchSize || !indexCursor.hasNext()) {
            return completedFuture(null);
        }

        RowId rowId = indexCursor.next();

        return resolvePlainReadResult(rowId, null, timestamp).thenComposeAsync(resolvedReadResult -> {
            if (resolvedReadResult != null) {
                result.add(resolvedReadResult);
            }

            return continueReadOnlyIndexLookup(indexCursor, timestamp, batchSize, result);
        }, scanRequestExecutor);
    }

    /**
     * Processes transaction finish request.
     * <ol>
     *     <li>Get commit timestamp from finish replica request.</li>
     *     <li>If attempting a commit, validate commit (and, if not valid, switch to abort)</li>
     *     <li>Run specific raft {@code FinishTxCommand} command, that will apply txn state to corresponding txStateStorage.</li>
     *     <li>Send cleanup requests to all enlisted primary replicas.</li>
     * </ol>
     *
     * @param request Transaction finish request.
     * @param txCoordinatorId Transaction coordinator id.
     * @return future result of the operation.
     */
    // TODO: need to properly handle primary replica changes https://issues.apache.org/jira/browse/IGNITE-17615
    private CompletableFuture<Void> processTxFinishAction(TxFinishReplicaRequest request, String txCoordinatorId) {
        List<TablePartitionId> aggregatedGroupIds = request.groups().values().stream()
                .flatMap(List::stream)
                .map(IgniteBiTuple::get1)
                .collect(toList());

        UUID txId = request.txId();

        if (request.commit()) {
            return schemaCompatValidator.validateForward(txId, aggregatedGroupIds, request.commitTimestamp())
                    .thenCompose(validationResult -> {
                        return finishAndCleanup(request, validationResult.isSuccessful(), aggregatedGroupIds, txId, txCoordinatorId)
                                .thenAccept(unused -> throwIfSchemaValidationOnCommitFailed(validationResult));
                    });
        } else {
            // Aborting.
            return finishAndCleanup(request, false, aggregatedGroupIds, txId, txCoordinatorId);
        }
    }

    private static void throwIfSchemaValidationOnCommitFailed(CompatValidationResult validationResult) {
        if (!validationResult.isSuccessful()) {
            throw new IncompatibleSchemaAbortException("Commit failed because schema "
                    + validationResult.fromSchemaVersion() + " is not forward-compatible with "
                    + validationResult.toSchemaVersion() + " for table " + validationResult.failedTableId());
        }
    }

    private CompletableFuture<Void> finishAndCleanup(
            TxFinishReplicaRequest request,
            boolean commit,
            List<TablePartitionId> aggregatedGroupIds,
            UUID txId,
            String txCoordinatorId
    ) {
        CompletableFuture<?> changeStateFuture = finishTransaction(aggregatedGroupIds, txId, commit, txCoordinatorId);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17578 Cleanup process should be asynchronous.
        CompletableFuture<?>[] cleanupFutures = new CompletableFuture[request.groups().size()];
        AtomicInteger cleanupFuturesCnt = new AtomicInteger(0);

        request.groups().forEach(
                (recipientNode, tablePartitionIds) ->
                        cleanupFutures[cleanupFuturesCnt.getAndIncrement()] = changeStateFuture.thenCompose(ignored ->
                                txManager.cleanup(
                                        recipientNode,
                                        tablePartitionIds,
                                        txId,
                                        commit,
                                        request.commitTimestamp()
                                )
                        )
        );

        return allOf(cleanupFutures);
    }

    /**
     * Finishes a transaction.
     *
     * @param aggregatedGroupIds Partition identifies which are enlisted in the transaction.
     * @param txId Transaction id.
     * @param commit True is the transaction is committed, false otherwise.
     * @param txCoordinatorId Transaction coordinator id.
     * @return Future to wait of the finish.
     */
    private CompletableFuture<Object> finishTransaction(
            List<TablePartitionId> aggregatedGroupIds,
            UUID txId,
            boolean commit,
            String txCoordinatorId
    ) {
        // TODO: IGNITE-20034 Timestamp from request is not using until the issue has not been fixed (request.commitTimestamp())
        var fut = new CompletableFuture<TxMeta>();

        txTimestampUpdateMap.put(txId, fut);

        HybridTimestamp currentTimestamp = hybridClock.now();
        HybridTimestamp commitTimestamp = commit ? currentTimestamp : null;

        return catalogVersionFor(currentTimestamp)
                .thenApply(catalogVersion -> {
                    FinishTxCommandBuilder finishTxCmdBldr = MSG_FACTORY.finishTxCommand()
                            .txId(txId)
                            .commit(commit)
                            .safeTimeLong(currentTimestamp.longValue())
                            .txCoordinatorId(txCoordinatorId)
                            .requiredCatalogVersion(catalogVersion)
                            .tablePartitionIds(
                                    aggregatedGroupIds.stream()
                                            .map(PartitionReplicaListener::tablePartitionId)
                                            .collect(toList())
                            );

                    if (commit) {
                        finishTxCmdBldr.commitTimestampLong(commitTimestamp.longValue());
                    }

                    return finishTxCmdBldr.build();
                })
                .thenCompose(raftClient::run)
                .whenComplete((o, throwable) -> {
                    TxState txState = commit ? COMMITED : ABORTED;

                    fut.complete(new TxMeta(txState, aggregatedGroupIds, commitTimestamp));

                    markFinished(txId, txState, commitTimestamp);

                    txTimestampUpdateMap.remove(txId);
                });
    }


    /**
     * Processes transaction cleanup request:
     * <ol>
     *     <li>Waits for finishing of local transactional operations;</li>
     *     <li>Runs asynchronously the specific raft {@code TxCleanupCommand} command, that will convert all pending entries(writeIntents)
     *     to either regular values({@link TxState#COMMITED}) or removing them ({@link TxState#ABORTED});</li>
     *     <li>Releases all locks that were held on local Replica by given transaction.</li>
     * </ol>
     * This operation is idempotent, so it's safe to retry it.
     *
     * @param request Transaction cleanup request.
     * @return CompletableFuture of void.
     */
    // TODO: need to properly handle primary replica changes https://issues.apache.org/jira/browse/IGNITE-17615
    private CompletableFuture<Void> processTxCleanupAction(TxCleanupReplicaRequest request) {
        try {
            closeAllTransactionCursors(request.txId());
        } catch (Exception e) {
            return failedFuture(e);
        }

        TxState txState = request.commit() ? COMMITED : ABORTED;

        markFinished(request.txId(), txState, request.commitTimestamp());

        List<CompletableFuture<?>> txUpdateFutures = new ArrayList<>();
        List<CompletableFuture<?>> txReadFutures = new ArrayList<>();

        // TODO https://issues.apache.org/jira/browse/IGNITE-18617
        txCleanupReadyFutures.compute(request.txId(), (id, txOps) -> {
            if (txOps == null) {
                txOps = new TxCleanupReadyFutureList();
            }

            txOps.futures.forEach((opType, futures) -> {
                if (opType == RequestType.RW_GET || opType == RequestType.RW_GET_ALL || opType == RequestType.RW_SCAN) {
                    txReadFutures.addAll(futures);
                } else {
                    txUpdateFutures.addAll(futures);
                }
            });

            txOps.futures.clear();

            txOps.state = txState;

            return txOps;
        });

        if (txUpdateFutures.isEmpty()) {
            if (!txReadFutures.isEmpty()) {
                return allOffFuturesExceptionIgnored(txReadFutures, request)
                        .thenRun(() -> releaseTxLocks(request.txId()));
            }

            return completedFuture(null);
        }

        return allOffFuturesExceptionIgnored(txUpdateFutures, request).thenCompose(v -> {
            long commandTimestamp = hybridClock.nowLong();

            return catalogVersionFor(hybridTimestamp(commandTimestamp))
                    .thenCompose(catalogVersion -> {
                        TxCleanupCommand txCleanupCmd = MSG_FACTORY.txCleanupCommand()
                                .txId(request.txId())
                                .commit(request.commit())
                                .commitTimestampLong(request.commitTimestampLong())
                                .safeTimeLong(commandTimestamp)
                                .txCoordinatorId(getTxCoordinatorId(request.txId()))
                                .requiredCatalogVersion(catalogVersion)
                                .build();

                        storageUpdateHandler.handleTransactionCleanup(request.txId(), request.commit(), request.commitTimestamp());

                        raftClient.run(txCleanupCmd)
                                .exceptionally(e -> {
                                    LOG.warn("Failed to complete transaction cleanup command [txId=" + request.txId() + ']', e);

                                    return completedFuture(null);
                                });

                        return allOffFuturesExceptionIgnored(txReadFutures, request)
                                .thenRun(() -> releaseTxLocks(request.txId()));
                    });
        });
    }

    private String getTxCoordinatorId(UUID txId) {
        TxStateMeta meta = txManager.stateMeta(txId);

        assert meta != null : "Trying to cleanup a transaction that was not enlisted, txId=" + txId;

        return meta.txCoordinatorId();
    }

    /**
     * Creates a future that waits all transaction operations are completed.
     *
     * @param txFutures Transaction operation futures.
     * @param request Cleanup request.
     * @return The future completes when all futures in passed list are completed.
     */
    private static CompletableFuture<Void> allOffFuturesExceptionIgnored(List<CompletableFuture<?>> txFutures,
            TxCleanupReplicaRequest request) {
        return allOf(txFutures.toArray(new CompletableFuture<?>[0]))
                .exceptionally(e -> {
                    assert !request.commit() :
                            "Transaction is committing, but an operation has completed with exception [txId=" + request.txId()
                                    + ", err=" + e.getMessage() + ']';

                    return null;
                });
    }

    private void releaseTxLocks(UUID txId) {
        lockManager.locks(txId).forEachRemaining(lockManager::release);
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
            BiFunction<@Nullable RowId, @Nullable BinaryRow, CompletableFuture<T>> action
    ) {
        IndexLocker pkLocker = indexesLockers.get().get(pkIndexStorage.get().id());

        assert pkLocker != null;

        return pkLocker.locksForLookupByKey(txId, pk)
                .thenCompose(ignored -> {

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
                });
    }

    private <T> CompletableFuture<T> continueResolvingByPk(
            Cursor<RowId> cursor,
            UUID txId,
            BiFunction<@Nullable RowId, @Nullable BinaryRow, CompletableFuture<T>> action
    ) {
        if (!cursor.hasNext()) {
            return action.apply(null, null);
        }

        RowId rowId = cursor.next();

        return resolvePlainReadResult(rowId, txId).thenCompose(row -> {
            if (row != null) {
                return action.apply(rowId, row);
            } else {
                return continueResolvingByPk(cursor, txId, action);
            }
        });

    }

    /**
     * Appends an operation to prevent the race between commit/rollback and the operation execution.
     *
     * @param txId Transaction id.
     * @param cmdType Command type.
     * @param full {@code True} if a full transaction and can be immediately committed.
     * @param op Operation closure.
     * @param <T> Type of execution result.
     * @return A future object representing the result of the given operation.
     */
    private <T> CompletableFuture<T> appendTxCommand(UUID txId, RequestType cmdType, boolean full, Supplier<CompletableFuture<T>> op) {
        var fut = new CompletableFuture<T>();

        if (!full) {
            txCleanupReadyFutures.compute(txId, (id, txOps) -> {
                if (txOps == null) {
                    txOps = new TxCleanupReadyFutureList();
                }

                if (txOps.state == ABORTED || txOps.state == COMMITED) {
                    fut.completeExceptionally(
                            new TransactionException(TX_FAILED_READ_WRITE_OPERATION_ERR, "Transaction is already finished."));
                } else {
                    txOps.futures.computeIfAbsent(cmdType, type -> new ArrayList<>()).add(fut);
                }

                return txOps;
            });
        }

        if (!fut.isDone()) {
            op.get().whenComplete((v, th) -> {
                if (full) { // Fast unlock.
                    releaseTxLocks(txId);
                }

                if (th != null) {
                    fut.completeExceptionally(th);
                } else {
                    fut.complete(v);
                }
            });
        }

        return fut;
    }

    /**
     * Finds the row and its identifier by given pk search row.
     *
     * @param pk Binary Tuple bytes representing a primary key.
     * @param ts A timestamp regarding which we need to resolve the given row.
     * @return Result of the given action.
     */
    private CompletableFuture<BinaryRow> resolveRowByPkForReadOnly(BinaryTuple pk, HybridTimestamp ts) {
        try (Cursor<RowId> cursor = getFromPkIndex(pk)) {
            List<ReadResult> candidates = new ArrayList<>();

            for (RowId rowId : cursor) {
                ReadResult readResult = mvDataStorage.read(rowId, ts);

                if (!readResult.isEmpty() || readResult.isWriteIntent()) {
                    candidates.add(readResult);
                }
            }

            if (candidates.isEmpty()) {
                return completedFuture(null);
            }

            // TODO https://issues.apache.org/jira/browse/IGNITE-18767 scan of multiple write intents should not be needed
            List<ReadResult> writeIntents = filter(candidates, ReadResult::isWriteIntent);

            if (!writeIntents.isEmpty()) {
                ReadResult writeIntent = writeIntents.get(0);

                // Assume that all write intents for the same key belong to the same transaction, as the key should be exclusively locked.
                // This means that we can just resolve the state of this transaction.
                checkWriteIntentsBelongSameTx(writeIntents);

                return resolveTxState(
                        new TablePartitionId(writeIntent.commitTableId(), writeIntent.commitPartitionId()),
                        writeIntent.transactionId(),
                        ts)
                        .thenApply(readLastCommitted -> {
                            if (readLastCommitted) {
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

                                return findAny(candidates, c -> !c.isWriteIntent() && !c.isEmpty()).map(ReadResult::binaryRow)
                                        .orElse(null);
                            } else {
                                return findAny(writeIntents, wi -> !wi.isEmpty()).map(ReadResult::binaryRow)
                                        .orElse(null);
                            }
                        });
            } else {
                BinaryRow result = findAny(candidates, r -> !r.isEmpty()).map(ReadResult::binaryRow)
                        .orElse(null);

                return completedFuture(result);
            }
        } catch (Exception e) {
            throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                    format("Unable to close cursor [tableId={}]", tableId()), e);
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

            assert Objects.equals(wi.commitTableId(), writeIntent.commitTableId())
                    : "Unexpected write intent, commitTableId1=" + writeIntent.commitTableId() + ", commitTableId2=" + wi.commitTableId();

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
     * Precesses multi request.
     *
     * @param request Multi request operation.
     * @param txCoordinatorId Transaction coordinator id.
     * @return Listener response.
     */
    private CompletableFuture<?> processMultiEntryAction(ReadWriteMultiRowReplicaRequest request, String txCoordinatorId) {
        UUID txId = request.transactionId();
        TablePartitionId committedPartitionId = request.commitPartitionId().asTablePartitionId();
        List<BinaryRow> searchRows = request.binaryRows();

        assert committedPartitionId != null : "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_DELETE_EXACT_ALL: {
                CompletableFuture<RowId>[] deleteExactLockFuts = new CompletableFuture[searchRows.size()];

                for (int i = 0; i < searchRows.size(); i++) {
                    BinaryRow searchRow = searchRows.get(i);

                    deleteExactLockFuts[i] = resolveRowByPk(extractPk(searchRow), txId, (rowId, row) -> {
                        if (rowId == null) {
                            return completedFuture(null);
                        }

                        return takeLocksForDeleteExact(searchRow, rowId, row, txId);
                    });
                }

                return allOf(deleteExactLockFuts).thenCompose(ignore -> {
                    Map<UUID, BinaryRowMessage> rowIdsToDelete = new HashMap<>();
                    Collection<BinaryRow> result = new ArrayList<>();

                    for (int i = 0; i < searchRows.size(); i++) {
                        RowId lockedRowId = deleteExactLockFuts[i].join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.put(lockedRowId.uuid(), null);
                        } else {
                            result.add(searchRows.get(i));
                        }
                    }

                    if (rowIdsToDelete.isEmpty()) {
                        return completedFuture(result);
                    }

                    return updateAllCommand(request, rowIdsToDelete, txCoordinatorId)
                                .thenCompose(this::applyUpdateAllCommand)
                                .thenApply(v -> result);
                });
            }
            case RW_INSERT_ALL: {
                List<BinaryTuple> pks = new ArrayList<>(searchRows.size());

                CompletableFuture<RowId>[] pkReadLockFuts = new CompletableFuture[searchRows.size()];

                for (int i = 0; i < searchRows.size(); i++) {
                    BinaryTuple pk = extractPk(searchRows.get(i));

                    pks.add(pk);

                    pkReadLockFuts[i] = resolveRowByPk(pk, txId, (rowId, row) -> completedFuture(rowId));
                }

                return allOf(pkReadLockFuts).thenCompose(ignore -> {
                    Collection<BinaryRow> result = new ArrayList<>();
                    Map<RowId, BinaryRow> rowsToInsert = new HashMap<>();
                    Set<ByteBuffer> uniqueKeys = new HashSet<>();

                    for (int i = 0; i < searchRows.size(); i++) {
                        BinaryRow row = searchRows.get(i);
                        RowId lockedRow = pkReadLockFuts[i].join();

                        if (lockedRow == null && uniqueKeys.add(pks.get(i).byteBuffer())) {
                            rowsToInsert.put(new RowId(partId(), UUID.randomUUID()), row);
                        } else {
                            result.add(row);
                        }
                    }

                    if (rowsToInsert.isEmpty()) {
                        return completedFuture(result);
                    }

                    CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>>[] insertLockFuts = new CompletableFuture[rowsToInsert.size()];

                    int idx = 0;

                    for (Map.Entry<RowId, BinaryRow> entry : rowsToInsert.entrySet()) {
                        insertLockFuts[idx++] = takeLocksForInsert(entry.getValue(), entry.getKey(), txId);
                    }

                    Map<UUID, BinaryRowMessage> convertedMap = rowsToInsert.entrySet().stream()
                            .collect(Collectors.toMap(
                                    e -> e.getKey().uuid(),
                                    e -> MSG_FACTORY.binaryRowMessage()
                                            .binaryTuple(e.getValue().tupleSlice())
                                            .schemaVersion(e.getValue().schemaVersion())
                                            .build()
                            ));

                    return allOf(insertLockFuts)
                            .thenCompose(ignored -> updateAllCommand(request, convertedMap, txCoordinatorId))
                            .thenCompose(this::applyUpdateAllCommand)
                            .thenApply(ignored -> {
                                // Release short term locks.
                                for (CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> insertLockFut : insertLockFuts) {
                                    insertLockFut.join().get2()
                                            .forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));
                                }

                                return result;
                            });
                });
            }
            case RW_UPSERT_ALL: {
                CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>>[] rowIdFuts = new CompletableFuture[searchRows.size()];

                for (int i = 0; i < searchRows.size(); i++) {
                    BinaryRow searchRow = searchRows.get(i);

                    rowIdFuts[i] = resolveRowByPk(extractPk(searchRow), txId, (rowId, row) -> {
                        boolean insert = rowId == null;

                        RowId rowId0 = insert ? new RowId(partId(), UUID.randomUUID()) : rowId;

                        return insert
                                ? takeLocksForInsert(searchRow, rowId0, txId)
                                : takeLocksForUpdate(searchRow, rowId0, txId);
                    });
                }

                return allOf(rowIdFuts).thenCompose(ignore -> {
                    List<BinaryRowMessage> searchRowMessages = request.binaryRowMessages();
                    Map<UUID, BinaryRowMessage> rowsToUpdate = IgniteUtils.newHashMap(searchRowMessages.size());

                    for (int i = 0; i < searchRowMessages.size(); i++) {
                        RowId lockedRow = rowIdFuts[i].join().get1();

                        rowsToUpdate.put(lockedRow.uuid(), searchRowMessages.get(i));
                    }

                    if (rowsToUpdate.isEmpty()) {
                        return completedFuture(null);
                    }

                    return updateAllCommand(request, rowsToUpdate, txCoordinatorId)
                            .thenCompose(this::applyUpdateAllCommand)
                            .thenRun(() -> {
                                // Release short term locks.
                                for (CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> rowIdFut : rowIdFuts) {
                                    rowIdFut.join().get2()
                                            .forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));
                                }
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
     * @param txCoordinatorId Transaction coordinator id.
     * @return Listener response.
     */
    private CompletableFuture<Object> processMultiEntryAction(ReadWriteMultiRowPkReplicaRequest request, String txCoordinatorId) {
        UUID txId = request.transactionId();
        TablePartitionId committedPartitionId = request.commitPartitionId().asTablePartitionId();
        List<BinaryTuple> primaryKeys = resolvePks(request.primaryKeys());

        assert committedPartitionId != null || request.requestType() == RequestType.RW_GET_ALL
                : "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_GET_ALL: {
                CompletableFuture<BinaryRow>[] rowFuts = new CompletableFuture[primaryKeys.size()];

                for (int i = 0; i < primaryKeys.size(); i++) {
                    rowFuts[i] = resolveRowByPk(primaryKeys.get(i), txId, (rowId, row) -> {
                        if (rowId == null) {
                            return completedFuture(null);
                        }

                        return takeLocksForGet(rowId, txId)
                                .thenApply(ignored -> row);
                    });
                }

                return allOf(rowFuts)
                        .thenApply(ignored -> {
                            var result = new ArrayList<BinaryRow>(primaryKeys.size());

                            for (CompletableFuture<BinaryRow> rowFut : rowFuts) {
                                result.add(rowFut.join());
                            }

                            return result;
                        });
            }
            case RW_DELETE_ALL: {
                CompletableFuture<RowId>[] rowIdLockFuts = new CompletableFuture[primaryKeys.size()];

                for (int i = 0; i < primaryKeys.size(); i++) {
                    rowIdLockFuts[i] = resolveRowByPk(primaryKeys.get(i), txId, (rowId, row) -> {
                        if (rowId == null) {
                            return completedFuture(null);
                        }

                        return takeLocksForDelete(row, rowId, txId);
                    });
                }

                return allOf(rowIdLockFuts).thenCompose(ignore -> {
                    Map<UUID, BinaryRowMessage> rowIdsToDelete = new HashMap<>();
                    Collection<Boolean> result = new ArrayList<>();

                    for (CompletableFuture<RowId> lockFut : rowIdLockFuts) {
                        RowId lockedRowId = lockFut.join();

                        if (lockedRowId != null) {
                            rowIdsToDelete.put(lockedRowId.uuid(), null);

                            result.add(true);
                        } else {
                            result.add(false);
                        }
                    }

                    if (rowIdsToDelete.isEmpty()) {
                        return completedFuture(result);
                    }

                    return updateAllCommand(request, rowIdsToDelete, txCoordinatorId)
                            .thenCompose(this::applyUpdateAllCommand)
                            .thenApply(ignored -> result);
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        format("Unknown multi request [actionType={}]", request.requestType()));
            }
        }
    }

    /**
     * Executes a command and handles exceptions. A result future can be finished with exception by following rules:
     * <ul>
     *     <li>If RAFT command cannot finish due to timeout, the future finished with {@link ReplicationTimeoutException}.</li>
     *     <li>If RAFT command finish with a runtime exception, the exception is moved to the result future.</li>
     *     <li>If RAFT command finish with any other exception, the future finished with {@link ReplicationException}.
     *     The original exception is set as cause.</li>
     * </ul>
     *
     * @param cmd Raft command.
     * @return Raft future.
     */
    private CompletableFuture<Object> applyCmdWithExceptionHandling(Command cmd) {
        return raftClient.run(cmd).exceptionally(throwable -> {
            if (throwable instanceof TimeoutException) {
                throw new ReplicationTimeoutException(replicationGroupId);
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new ReplicationException(replicationGroupId, throwable);
            }
        });
    }

    /**
     * Executes an Update command.
     *
     * @param cmd Update command.
     * @return Raft future, see {@link #applyCmdWithExceptionHandling(Command)}.
     */
    private CompletableFuture<Object> applyUpdateCommand(UpdateCommand cmd) {
        if (!cmd.full()) {
            storageUpdateHandler.handleUpdate(
                    cmd.txId(),
                    cmd.rowUuid(),
                    cmd.tablePartitionId().asTablePartitionId(),
                    cmd.row(),
                    true,
                    null,
                    null);
        }

        return applyCmdWithExceptionHandling(cmd).thenApply(res -> {
            // Try to avoid double write if an entry is already replicated.
            if (cmd.full() && cmd.safeTime().compareTo(safeTime.current()) > 0) {
                storageUpdateHandler.handleUpdate(
                        cmd.txId(),
                        cmd.rowUuid(),
                        cmd.tablePartitionId().asTablePartitionId(),
                        cmd.row(),
                        false,
                        null,
                        cmd.safeTime());
            }

            return res;
        });
    }

    /**
     * Executes an UpdateAll command.
     *
     * @param cmd UpdateAll command.
     * @return Raft future, see {@link #applyCmdWithExceptionHandling(Command)}.
     */
    private CompletableFuture<Object> applyUpdateAllCommand(UpdateAllCommand cmd) {
        if (!cmd.full()) {
            storageUpdateHandler.handleUpdateAll(
                    cmd.txId(),
                    cmd.rowsToUpdate(),
                    cmd.tablePartitionId().asTablePartitionId(),
                    true,
                    null,
                    null);
        }

        return applyCmdWithExceptionHandling(cmd).thenApply(res -> {
            if (cmd.full() && cmd.safeTime().compareTo(safeTime.current()) > 0) {
                storageUpdateHandler.handleUpdateAll(
                        cmd.txId(),
                        cmd.rowsToUpdate(),
                        cmd.tablePartitionId().asTablePartitionId(),
                        false,
                        null,
                        cmd.safeTime());
            }

            return res;
        });
    }

    /**
     * Precesses single request.
     *
     * @param request Single request operation.
     * @param txCoordinatorId Transaction coordinator id.
     * @return Listener response.
     */
    private CompletableFuture<?> processSingleEntryAction(ReadWriteSingleRowReplicaRequest request, String txCoordinatorId) {
        UUID txId = request.transactionId();
        BinaryRow searchRow = request.binaryRow();
        TablePartitionId commitPartitionId = request.commitPartitionId().asTablePartitionId();

        assert commitPartitionId != null : "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_DELETE_EXACT: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(false);
                    }

                    return takeLocksForDeleteExact(searchRow, rowId, row, txId)
                            .thenCompose(validatedRowId -> {
                                if (validatedRowId == null) {
                                    return completedFuture(false);
                                }

                                return updateCommand(request, validatedRowId.uuid(), null, txCoordinatorId)
                                        .thenCompose(this::applyUpdateCommand)
                                        .thenApply(ignored -> true);
                            });
                });
            }
            case RW_INSERT: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row) -> {
                    if (rowId != null) {
                        return completedFuture(false);
                    }

                    RowId rowId0 = new RowId(partId(), UUID.randomUUID());

                    return takeLocksForInsert(searchRow, rowId0, txId)
                            .thenCompose(rowIdLock -> updateCommand(request, rowId0.uuid(), searchRow, txCoordinatorId)
                                    .thenCompose(this::applyUpdateCommand)
                                    .thenApply(v -> {
                                        // Release short term locks.
                                        rowIdLock.get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                        return true;
                                    }));
                });
            }
            case RW_UPSERT: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row) -> {
                    boolean insert = rowId == null;

                    RowId rowId0 = insert ? new RowId(partId(), UUID.randomUUID()) : rowId;

                    CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> lockFut = insert
                            ? takeLocksForInsert(searchRow, rowId0, txId)
                            : takeLocksForUpdate(searchRow, rowId0, txId);

                    return lockFut
                            .thenCompose(rowIdLock -> updateCommand(request, rowId0.uuid(), searchRow, txCoordinatorId)
                                    .thenCompose(this::applyUpdateCommand)
                                    .thenRun(() -> {
                                        // Release short term locks.
                                        rowIdLock.get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));
                                    }));
                });
            }
            case RW_GET_AND_UPSERT: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row) -> {
                    boolean insert = rowId == null;

                    RowId rowId0 = insert ? new RowId(partId(), UUID.randomUUID()) : rowId;

                    CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> lockFut = insert
                            ? takeLocksForInsert(searchRow, rowId0, txId)
                            : takeLocksForUpdate(searchRow, rowId0, txId);

                    return lockFut
                            .thenCompose(rowIdLock -> updateCommand(request, rowId0.uuid(), searchRow, txCoordinatorId)
                                    .thenCompose(this::applyUpdateCommand)
                                    .thenApply(v -> {
                                        // Release short term locks.
                                        rowIdLock.get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                        return row;
                                    }));
                });
            }
            case RW_GET_AND_REPLACE: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(null);
                    }

                    return takeLocksForUpdate(searchRow, rowId, txId)
                            .thenCompose(rowIdLock -> updateCommand(request, rowId.uuid(), searchRow, txCoordinatorId)
                                    .thenCompose(this::applyUpdateCommand)
                                    .thenApply(v -> {
                                        // Release short term locks.
                                        rowIdLock.get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                        return row;
                                    }));
                });
            }
            case RW_REPLACE_IF_EXIST: {
                return resolveRowByPk(extractPk(searchRow), txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(false);
                    }

                    return takeLocksForUpdate(searchRow, rowId, txId)
                            .thenCompose(rowIdLock -> updateCommand(request, rowId.uuid(), searchRow, txCoordinatorId)
                                    .thenCompose(this::applyUpdateCommand)
                                    .thenApply(v -> {
                                        // Release short term locks.
                                        rowIdLock.get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                        return true;
                                    }));
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
     * @param txCoordinatorId Transaction coordinator id.
     * @return Listener response.
     */
    private CompletableFuture<?> processSingleEntryAction(ReadWriteSingleRowPkReplicaRequest request, String txCoordinatorId) {
        UUID txId = request.transactionId();
        BinaryTuple primaryKey = resolvePk(request.primaryKey());
        TablePartitionId commitPartitionId = request.commitPartitionId().asTablePartitionId();

        assert commitPartitionId != null || request.requestType() == RequestType.RW_GET :
                "Commit partition is null [type=" + request.requestType() + ']';

        switch (request.requestType()) {
            case RW_GET: {
                return resolveRowByPk(primaryKey, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(null);
                    }

                    return takeLocksForGet(rowId, txId)
                            .thenApply(ignored -> row);
                });
            }
            case RW_DELETE: {
                return resolveRowByPk(primaryKey, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(false);
                    }

                    return takeLocksForDelete(row, rowId, txId)
                            .thenCompose(ignored -> updateCommand(request, rowId.uuid(), null, txCoordinatorId))
                            .thenCompose(this::applyUpdateCommand)
                            .thenApply(ignored -> true);
                });
            }
            case RW_GET_AND_DELETE: {
                return resolveRowByPk(primaryKey, txId, (rowId, row) -> {
                    if (rowId == null) {
                        return completedFuture(null);
                    }

                    return takeLocksForDelete(row, rowId, txId)
                            .thenCompose(ignored -> updateCommand(request, rowId.uuid(), null, txCoordinatorId))
                            .thenCompose(this::applyUpdateCommand)
                            .thenApply(ignored -> row);
                });
            }
            default: {
                throw new IgniteInternalException(Replicator.REPLICA_COMMON_ERR,
                        format("Unknown single request [actionType={}]", request.requestType()));
            }
        }
    }

    private BinaryTuple extractPk(BinaryRow row) {
        return pkIndexStorage.get().indexRowResolver().extractColumns(row);
    }

    private BinaryTuple resolvePk(ByteBuffer bytes) {
        return new BinaryTuple(pkLength, bytes);
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
        return lockManager.acquire(txId, new LockKey(tableId()), LockMode.IX)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.X))
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
        return lockManager.acquire(txId, new LockKey(tableId()), LockMode.IX) // IX lock on table
                .thenCompose(ignored -> takePutLockOnIndexes(binaryRow, rowId, txId))
                .thenApply(shortTermLocks -> new IgniteBiTuple<>(rowId, shortTermLocks));
    }

    private CompletableFuture<Collection<Lock>> takePutLockOnIndexes(BinaryRow binaryRow, RowId rowId, UUID txId) {
        Collection<IndexLocker> indexes = indexesLockers.get().values();

        if (nullOrEmpty(indexes)) {
            return completedFuture(Collections.emptyList());
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
            return completedFuture(null);
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
        return lockManager.acquire(txId, new LockKey(tableId()), LockMode.IX) // IX lock on table
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.S)) // S lock on RowId
                .thenCompose(ignored -> {
                    if (equalValues(actualRow, expectedRow)) {
                        return lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.X) // X lock on RowId
                                .thenCompose(ignored0 -> takeRemoveLockOnIndexes(actualRow, rowId, txId))
                                .thenApply(exclusiveRowLock -> rowId);
                    }

                    return completedFuture(null);
                });
    }

    /**
     * Takes all required locks on a key, before deleting the value.
     *
     * @param txId Transaction id.
     * @return Future completes with {@link RowId} or {@code null} if there is no value for the key.
     */
    private CompletableFuture<RowId> takeLocksForDelete(BinaryRow binaryRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId()), LockMode.IX) // IX lock on table
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.X)) // X lock on RowId
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
        return lockManager.acquire(txId, new LockKey(tableId()), LockMode.IS) // IS lock on table
                .thenCompose(tblLock -> lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.S)) // S lock on RowId
                .thenApply(ignored -> rowId);
    }

    /**
     * Precesses two actions.
     *
     * @param request Two actions operation request.
     * @param txCoordinatorId Transaction coordinator id.
     * @return Listener response.
     */
    private CompletableFuture<Boolean> processTwoEntriesAction(ReadWriteSwapRowReplicaRequest request, String txCoordinatorId) {
        BinaryRow newRow = request.binaryRow();
        BinaryRow expectedRow = request.oldBinaryRow();
        TablePartitionIdMessage commitPartitionId = request.commitPartitionId();

        assert commitPartitionId != null : "Commit partition is null [type=" + request.requestType() + ']';

        UUID txId = request.transactionId();

        if (request.requestType() == RequestType.RW_REPLACE) {
            return resolveRowByPk(extractPk(newRow), txId, (rowId, row) -> {
                if (rowId == null) {
                    return completedFuture(false);
                }

                return takeLocksForReplace(expectedRow, row, newRow, rowId, txId)
                        .thenCompose(validatedRowId -> {
                            if (validatedRowId == null) {
                                return completedFuture(false);
                            }

                            return updateCommand(commitPartitionId, validatedRowId.get1().uuid(), newRow, txId, request.full(),
                                        txCoordinatorId
                                    )
                                    .thenCompose(this::applyUpdateCommand)
                                    .thenApply(ignored -> validatedRowId)
                                    .thenApply(rowIdLock -> {
                                        // Release short term locks.
                                        rowIdLock.get2().forEach(lock -> lockManager.release(lock.txId(), lock.lockKey(), lock.lockMode()));

                                        return true;
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
    private CompletableFuture<IgniteBiTuple<RowId, Collection<Lock>>> takeLocksForReplace(BinaryRow expectedRow, BinaryRow oldRow,
            BinaryRow newRow, RowId rowId, UUID txId) {
        return lockManager.acquire(txId, new LockKey(tableId()), LockMode.IX)
                .thenCompose(ignored -> lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.S))
                .thenCompose(ignored -> {
                    if (oldRow != null && equalValues(oldRow, expectedRow)) {
                        return lockManager.acquire(txId, new LockKey(tableId(), rowId), LockMode.X) // X lock on RowId
                                .thenCompose(ignored1 -> takePutLockOnIndexes(newRow, rowId, txId))
                                .thenApply(shortTermLocks -> new IgniteBiTuple<>(rowId, shortTermLocks));
                    }

                    return completedFuture(null);
                });
    }

    /**
     * Ensure that the primary replica was not changed.
     *
     * @param request Replica request.
     * @return Future. The result is not null only for {@link ReadOnlyReplicaRequest}. If {@code true}, then replica is primary.
     */
    private CompletableFuture<Boolean> ensureReplicaIsPrimary(ReplicaRequest request) {
        Long expectedTerm;

        if (request instanceof ReadWriteReplicaRequest) {
            expectedTerm = ((ReadWriteReplicaRequest) request).term();

            assert expectedTerm != null;
        } else if (request instanceof TxFinishReplicaRequest) {
            expectedTerm = ((TxFinishReplicaRequest) request).term();

            assert expectedTerm != null;
        } else if (request instanceof TxCleanupReplicaRequest) {
            expectedTerm = ((TxCleanupReplicaRequest) request).term();

            assert expectedTerm != null;
        } else {
            expectedTerm = null;
        }

        if (expectedTerm != null) {
            return raftClient.refreshAndGetLeaderWithTerm()
                    .thenCompose(replicaAndTerm -> {
                                long currentTerm = replicaAndTerm.term();

                                if (expectedTerm == currentTerm) {
                                    return completedFuture(null);
                                } else {
                                    return failedFuture(new PrimaryReplicaMissException(expectedTerm, currentTerm));
                                }
                            }
                    );
        } else if (request instanceof ReadOnlyReplicaRequest || request instanceof ReplicaSafeTimeSyncRequest) {
            return raftClient.refreshAndGetLeaderWithTerm().thenApply(replicaAndTerm -> isLocalPeer(replicaAndTerm.leader()));
        } else {
            return completedFuture(null);
        }
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
    private CompletableFuture<BinaryRow> resolveReadResult(
            ReadResult readResult,
            @Nullable UUID txId,
            @Nullable HybridTimestamp timestamp,
            Supplier<BinaryRow> lastCommitted
    ) {
        if (readResult == null) {
            return completedFuture(null);
        } else if (!readResult.isWriteIntent()) {
            return completedFuture(readResult.binaryRow());
        } else {
            // RW resolution.
            if (timestamp == null) {
                UUID retrievedResultTxId = readResult.transactionId();

                if (txId.equals(retrievedResultTxId)) {
                    // Same transaction - return retrieved value. It may be both writeIntent or regular value.
                    return completedFuture(readResult.binaryRow());
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
    private CompletableFuture<BinaryRow> resolveWriteIntentAsync(
            ReadResult readResult,
            HybridTimestamp timestamp,
            Supplier<BinaryRow> lastCommitted
    ) {
        return resolveTxState(
                new TablePartitionId(readResult.commitTableId(), readResult.commitPartitionId()),
                readResult.transactionId(),
                timestamp
        ).thenApply(readLastCommitted -> {
            if (readLastCommitted) {
                return lastCommitted.get();
            } else {
                return readResult.binaryRow();
            }
        });
    }

    /**
     * Resolve the actual tx state.
     *
     * @param commitGrpId Commit partition id.
     * @param txId Transaction id.
     * @param timestamp Timestamp.
     * @return The future completes with true when the transaction is not completed yet and false otherwise.
     */
    private CompletableFuture<Boolean> resolveTxState(
            TablePartitionId commitGrpId,
            UUID txId,
            HybridTimestamp timestamp
    ) {
        boolean readLatest = timestamp == null;

        return placementDriver.sendMetaRequest(commitGrpId, FACTORY.txStateReplicaRequest()
                        .groupId(commitGrpId)
                        .readTimestampLong((readLatest ? HybridTimestamp.MIN_VALUE : timestamp).longValue())
                        .txId(txId)
                        .build())
                .thenApply(txMeta -> {
                    if (txMeta == null) {
                        return true;
                    } else if (txMeta.txState() == COMMITED) {
                        return !readLatest && txMeta.commitTimestamp().compareTo(timestamp) > 0;
                    } else {
                        assert txMeta.txState() == ABORTED : "Unexpected transaction state [state=" + txMeta.txState() + ']';

                        return true;
                    }
                });
    }

    private CompletableFuture<UpdateCommand> updateCommand(
            ReadWriteSingleRowReplicaRequest request,
            UUID rowUuid,
            @Nullable BinaryRow row,
            String txCoordinatorId
    ) {
        return updateCommand(request.commitPartitionId(), rowUuid, row, request.transactionId(), request.full(), txCoordinatorId);
    }

    private CompletableFuture<UpdateCommand> updateCommand(
            ReadWriteSingleRowPkReplicaRequest request,
            UUID rowUuid,
            @Nullable BinaryRow row,
            String txCoordinatorId
    ) {
        return updateCommand(request.commitPartitionId(), rowUuid, row, request.transactionId(), request.full(), txCoordinatorId);
    }

    /**
     * Method to construct {@link UpdateCommand} object.
     *
     * @param tablePartId {@link TablePartitionId} object to construct {@link UpdateCommand} object with.
     * @param rowUuid Row UUID.
     * @param row Row.
     * @param txId Transaction ID.
     * @param full {@code True} if this is a full transaction.
     * @param txCoordinatorId Transaction coordinator id.
     * @return Constructed {@link UpdateCommand} object.
     */
    private CompletableFuture<UpdateCommand> updateCommand(
            TablePartitionIdMessage tablePartId,
            UUID rowUuid,
            @Nullable BinaryRow row,
            UUID txId,
            boolean full,
            String txCoordinatorId
    ) {
        long commandTimestamp = hybridClock.nowLong();

        return catalogVersionFor(hybridTimestamp(commandTimestamp))
                .thenApply(catalogVersion -> {
                    UpdateCommandBuilder bldr = MSG_FACTORY.updateCommand()
                            .tablePartitionId(tablePartId)
                            .rowUuid(rowUuid)
                            .txId(txId)
                            .full(full)
                            .safeTimeLong(commandTimestamp)
                            .txCoordinatorId(txCoordinatorId)
                            .requiredCatalogVersion(catalogVersion);

                    if (row != null) {
                        BinaryRowMessage rowMessage = MSG_FACTORY.binaryRowMessage()
                                .binaryTuple(row.tupleSlice())
                                .schemaVersion(row.schemaVersion())
                                .build();

                        bldr.rowMessage(rowMessage);
                    }

                    return bldr.build();
                });
    }

    private CompletableFuture<Integer> catalogVersionFor(HybridTimestamp ts) {
        return schemaSyncService.waitForMetadataCompleteness(ts)
                .thenApply(unused -> catalogService.activeCatalogVersion(ts.longValue()));
    }

    private CompletableFuture<UpdateAllCommand> updateAllCommand(
            ReadWriteMultiRowReplicaRequest request,
            Map<UUID, BinaryRowMessage> rowsToUpdate,
            String txCoordinatorId
    ) {
        return updateAllCommand(rowsToUpdate, request.commitPartitionId(), request.transactionId(), request.full(), txCoordinatorId);
    }

    private CompletableFuture<UpdateAllCommand> updateAllCommand(
            ReadWriteMultiRowPkReplicaRequest request,
            Map<UUID, BinaryRowMessage> rowsToUpdate,
            String txCoordinatorId
    ) {
        return updateAllCommand(rowsToUpdate, request.commitPartitionId(), request.transactionId(), request.full(), txCoordinatorId);
    }

    /**
     * Method to construct {@link UpdateAllCommand} object.
     *
     * @param rowsToUpdate All {@link BinaryRow}s represented as {@link ByteBuffer}s to be updated.
     * @param commitPartitionId Partition ID that these rows belong to.
     * @param transactionId Transaction ID.
     * @param full {@code true} if this is a single-command transaction.
     * @param txCoordinatorId Transaction coordinator id.
     * @return Constructed {@link UpdateAllCommand} object.
     */
    private CompletableFuture<UpdateAllCommand> updateAllCommand(
            Map<UUID, BinaryRowMessage> rowsToUpdate,
            TablePartitionIdMessage commitPartitionId,
            UUID transactionId,
            boolean full,
            String txCoordinatorId
    ) {
        long commandTimestamp = hybridClock.nowLong();

        return catalogVersionFor(hybridTimestamp(commandTimestamp))
                .thenApply(catalogVersion -> MSG_FACTORY.updateAllCommand()
                        .tablePartitionId(commitPartitionId)
                        .rowsToUpdate(rowsToUpdate)
                        .txId(transactionId)
                        .safeTimeLong(commandTimestamp)
                        .full(full)
                        .txCoordinatorId(txCoordinatorId)
                        .requiredCatalogVersion(catalogVersion)
                        .build()
                );
    }

    /**
     * Method to convert from {@link TablePartitionId} object to command-based {@link TablePartitionIdMessage} object.
     *
     * @param tablePartId {@link TablePartitionId} object to convert to {@link TablePartitionIdMessage}.
     * @return {@link TablePartitionIdMessage} object converted from argument.
     */
    public static TablePartitionIdMessage tablePartitionId(TablePartitionId tablePartId) {
        return MSG_FACTORY.tablePartitionIdMessage()
                .tableId(tablePartId.tableId())
                .partitionId(tablePartId.partitionId())
                .build();
    }

    /**
     * Class that stores a list of futures for operations that has happened in a specific transaction. Also, the class has a property
     * {@code state} that represents a transaction state.
     */
    private static class TxCleanupReadyFutureList {
        /**
         * Operation type is mapped operation futures.
         */
        final Map<RequestType, List<CompletableFuture<?>>> futures = new EnumMap<>(RequestType.class);

        /**
         * Transaction state. {@code TxState#ABORTED} and {@code TxState#COMMITED} match the final transaction states. If the property is
         * {@code null} the transaction is in pending state.
         */
        TxState state;
    }

    @Override
    public void onBecomePrimary(ClusterNode clusterNode) {
        inBusyLock(() -> {
            if (clusterNode.equals(localNode)) {
                if (primary) {
                    // Current replica has already become the primary, we do not need to do anything.
                    return;
                }

                primary = true;

                startBuildIndexes();
            } else {
                if (!primary) {
                    // Current replica was not the primary replica, we do not need to do anything.
                    return;
                }

                primary = false;

                stopBuildIndexes();
            }
        });
    }

    @Override
    public void onShutdown() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        stopBuildIndexes();
    }

    private void registerIndexesListener() {
        // TODO: IGNITE-19498 Might need to listen to something else
        ConfigurationNamedListListener<TableIndexView> listener = new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<TableIndexView> ctx) {
                inBusyLock(() -> {
                    TableIndexView indexView = ctx.newValue();

                    if (tableId() != indexView.tableId()) {
                        return;
                    }

                    TableView tableView = findTableView(ctx.newValue(TablesView.class), tableId());

                    assert tableView != null : tableId();

                    CatalogTableDescriptor catalogTableDescriptor = toTableDescriptor(tableView);
                    CatalogIndexDescriptor catalogIndexDescriptor = toIndexDescriptor(indexView);

                    startBuildIndex(StorageIndexDescriptor.create(catalogTableDescriptor, catalogIndexDescriptor));
                });

                return completedFuture(null);
            }

            @Override
            public CompletableFuture<?> onRename(ConfigurationNotificationEvent<TableIndexView> ctx) {
                return failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<TableIndexView> ctx) {
                inBusyLock(() -> {
                    TableIndexView tableIndexView = ctx.oldValue();

                    if (tableId() == tableIndexView.tableId()) {
                        indexBuilder.stopBuildIndex(tableId(), partId(), tableIndexView.id());
                    }
                });

                return completedFuture(null);
            }

            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<TableIndexView> ctx) {
                return failedFuture(new UnsupportedOperationException());
            }
        };

        boolean casResult = indexesConfigurationListener.compareAndSet(null, listener);

        assert casResult : replicationGroupId;

        tablesConfig.indexes().listenElements(listener);
    }

    private void startBuildIndex(StorageIndexDescriptor indexDescriptor) {
        // TODO: IGNITE-19112 We only need to create the index storage once
        IndexStorage indexStorage = mvTableStorage.getOrCreateIndex(partId(), indexDescriptor);

        indexBuilder.startBuildIndex(tableId(), partId(), indexDescriptor.id(), indexStorage, mvDataStorage, raftClient);
    }

    private int partId() {
        return replicationGroupId.partitionId();
    }

    private int tableId() {
        return replicationGroupId.tableId();
    }

    private boolean isLocalPeer(Peer peer) {
        return peer.consistentId().equals(localNode.name());
    }

    private void inBusyLock(Runnable runnable) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            runnable.run();
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void startBuildIndexes() {
        registerIndexesListener();

        // Let's try to build an index for the previously created indexes for the table.
        TablesView tablesView = tablesConfig.value();

        for (TableIndexView indexView : tablesView.indexes()) {
            if (indexView.tableId() != tableId()) {
                continue;
            }

            TableView tableView = findTableView(tablesView, tableId());

            assert tableView != null : tableId();

            CatalogTableDescriptor catalogTableDescriptor = toTableDescriptor(tableView);
            CatalogIndexDescriptor catalogIndexDescriptor = toIndexDescriptor(indexView);

            startBuildIndex(StorageIndexDescriptor.create(catalogTableDescriptor, catalogIndexDescriptor));
        }
    }

    private void stopBuildIndexes() {
        ConfigurationNamedListListener<TableIndexView> listener = indexesConfigurationListener.getAndSet(null);

        if (listener != null) {
            tablesConfig.indexes().stopListenElements(listener);
        }

        indexBuilder.stopBuildIndexes(tableId(), partId());
    }

    /**
     * Marks the transaction as finished in local tx state map.
     *
     * @param txId Transaction id.
     * @param txState Transaction state, must be either {@link TxState#COMMITED} or {@link TxState#ABORTED}.
     * @param commitTimestamp Commit timestamp.
     */
    private void markFinished(UUID txId, TxState txState, @Nullable HybridTimestamp commitTimestamp) {
        assert txState == COMMITED || txState == ABORTED : "Unexpected state, txId=" + txId + ", txState=" + txState;

        txManager.updateTxMeta(txId, old -> old == null
                ? null
                : new TxStateMeta(txState, old.txCoordinatorId(), txState == COMMITED ? commitTimestamp : null));
    }
}
