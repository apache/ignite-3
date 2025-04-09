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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.anyOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkResponse;
import org.apache.ignite.internal.lowwatermark.message.LowWatermarkMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.raft.PartitionSnapshotMeta;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataResponse;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.raft.PartitionSnapshotInfo;
import org.apache.ignite.internal.partition.replicator.raft.PartitionSnapshotInfoSerializer;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationSerializer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot copier implementation for partitions. Used to stream partition data from the leader to the local node.
 */
public class IncomingSnapshotCopier extends SnapshotCopier {
    private static final IgniteLogger LOG = Loggers.forClass(IncomingSnapshotCopier.class);

    private static final PartitionReplicationMessagesFactory TABLE_MSG_FACTORY = new PartitionReplicationMessagesFactory();

    private static final LowWatermarkMessagesFactory LWM_MSG_FACTORY = new LowWatermarkMessagesFactory();

    private static final long NETWORK_TIMEOUT = Long.MAX_VALUE;

    private static final long MAX_MV_DATA_PAYLOADS_BATCH_BYTES_HINT = 100 * 1024;

    private static final int MAX_TX_DATA_BATCH_SIZE = 1000;

    private final PartitionSnapshotStorage partitionSnapshotStorage;

    private final SnapshotUri snapshotUri;

    private final long waitForMetadataCatchupMs;

    /** Used to make sure that we execute cancellation at most once. */
    private final AtomicBoolean cancellationGuard = new AtomicBoolean();

    /** Busy lock for synchronous rebalance cancellation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final Executor executor;

    @Nullable
    private volatile CompletableFuture<SnapshotContext> snapshotMetaFuture;

    @Nullable
    private volatile CompletableFuture<Void> rebalanceFuture;

    /**
     * Future is to wait in {@link #join()} because it is important for us to wait for the rebalance to finish or abort.
     */
    @Nullable
    private volatile CompletableFuture<Void> joinFuture;

    /**
     * Constructor.
     *
     * @param partitionSnapshotStorage Snapshot storage.
     * @param snapshotUri Snapshot URI.
     * @param executor Thread pool for IO operations.
     * @param waitForMetadataCatchupMs How much time to allow for metadata on this node to reach the catalog version required by an
     *         incoming snapshot.
     */
    public IncomingSnapshotCopier(
            PartitionSnapshotStorage partitionSnapshotStorage,
            SnapshotUri snapshotUri,
            Executor executor,
            long waitForMetadataCatchupMs
    ) {
        this.partitionSnapshotStorage = partitionSnapshotStorage;
        this.snapshotUri = snapshotUri;
        this.executor = executor;
        this.waitForMetadataCatchupMs = waitForMetadataCatchupMs;
    }

    @Override
    public void start() {
        LOG.info("Copier is started for the partition [{}]", createPartitionInfo());

        ClusterNode snapshotSender = getSnapshotSender(snapshotUri.nodeName);

        CompletableFuture<SnapshotContext> metadataSufficiencyFuture;

        if (snapshotSender == null) {
            metadataSufficiencyFuture = failedFuture(new StorageRebalanceException("Snapshot sender not found: " + snapshotUri.nodeName));
        } else {
            metadataSufficiencyFuture = loadSnapshotMeta(snapshotSender)
                    // Give metadata some time to catch up as it's very probable that the leader is ahead metadata-wise.
                    .thenCompose(snapshotMeta -> waitForMetadataWithTimeout(snapshotMeta)
                            .thenApply(unused -> {
                                boolean metadataIsSufficientlyComplete = metadataIsSufficientlyComplete(snapshotMeta);

                                if (!metadataIsSufficientlyComplete) {
                                    logMetadataInsufficiencyAndSetError(snapshotMeta);

                                    return null;
                                } else {
                                    return new SnapshotContext(snapshotMeta, partitionSnapshotStorage.partitionsByTableId());
                                }
                            })
                    );
        }

        this.snapshotMetaFuture = metadataSufficiencyFuture;

        CompletableFuture<Void> rebalanceFuture = metadataSufficiencyFuture
                .thenComposeAsync(snapshotContext -> {
                    if (snapshotContext == null) {
                        return nullCompletedFuture();
                    }

                    assert snapshotSender != null : createPartitionInfo();

                    return startRebalance(snapshotContext)
                            .thenCompose(v -> loadSnapshotMvData(snapshotContext, snapshotSender))
                            .thenCompose(v -> loadSnapshotTxData(snapshotSender))
                            .thenRunAsync(() -> setNextRowIdToBuildIndexes(snapshotContext), executor);
                }, executor);

        this.rebalanceFuture = rebalanceFuture;

        this.joinFuture = metadataSufficiencyFuture
                .thenCompose(snapshotContext -> {
                    if (snapshotContext == null) {
                        return nullCompletedFuture();
                    }

                    assert snapshotSender != null : createPartitionInfo();

                    return rebalanceFuture
                            .handleAsync((v, throwable) -> completeRebalance(snapshotContext, throwable), executor)
                            .thenCompose(Function.identity())
                            .thenCompose(v -> tryUpdateLowWatermark(snapshotContext, snapshotSender));
                });
    }

    private CompletableFuture<?> waitForMetadataWithTimeout(PartitionSnapshotMeta snapshotMeta) {
        CompletableFuture<?> metadataReadyFuture = partitionSnapshotStorage.catalogService()
                .catalogReadyFuture(snapshotMeta.requiredCatalogVersion());
        CompletableFuture<?> readinessTimeoutFuture = completeOnMetadataReadinessTimeout();

        return anyOf(metadataReadyFuture, readinessTimeoutFuture);
    }

    private CompletableFuture<?> completeOnMetadataReadinessTimeout() {
        return new CompletableFuture<>()
                .orTimeout(waitForMetadataCatchupMs, TimeUnit.MILLISECONDS)
                .exceptionally(ex -> {
                    assert (ex instanceof TimeoutException);

                    return null;
                });
    }

    @Override
    public void join() throws InterruptedException {
        CompletableFuture<?> fut = joinFuture;

        if (fut != null) {
            try {
                fut.get();
            } catch (CancellationException ignored) {
                // Ignored.
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (!(cause instanceof CancellationException)) {
                    partitionSnapshotStorage.failureProcessor().process(new FailureContext(e, "Error when completing the copier"));

                    if (isOk()) {
                        setError(RaftError.UNKNOWN, "Unknown error on completion the copier");
                    }

                    // By analogy with LocalSnapshotCopier#join.
                    throw new IllegalStateException(cause);
                }
            }
        }
    }

    @Override
    public void cancel() {
        // Cancellation from one thread must not block cancellations from other threads, hence this check.
        if (!cancellationGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        LOG.info("Copier is canceled for partition [{}]", createPartitionInfo());

        // Cancel all futures that might be upstream wrt joinFuture.
        List<CompletableFuture<?>> futuresToCancel = Stream.of(snapshotMetaFuture, rebalanceFuture)
                .filter(Objects::nonNull)
                .collect(toList());

        futuresToCancel.forEach(future -> future.cancel(false));

        if (!futuresToCancel.isEmpty()) {
            try {
                // Because after the cancellation, no one waits for #join.
                join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public SnapshotReader getReader() {
        CompletableFuture<SnapshotContext> snapshotMetaFuture = this.snapshotMetaFuture;

        // This one's called when "join" is complete.
        assert snapshotMetaFuture != null && snapshotMetaFuture.isDone();

        SnapshotContext context = snapshotMetaFuture.isCompletedExceptionally() ? null : snapshotMetaFuture.join();

        // Use 'null' if we failed or were cancelled, this is what JRaft expects.
        return new IncomingSnapshotReader(context == null ? null : context.meta);
    }

    private @Nullable ClusterNode getSnapshotSender(String nodeName) {
        return partitionSnapshotStorage.topologyService().getByConsistentId(nodeName);
    }

    /**
     * Requests the snapshot meta.
     */
    private CompletableFuture<PartitionSnapshotMeta> loadSnapshotMeta(ClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.messagingService().invoke(
                    snapshotSender,
                    TABLE_MSG_FACTORY.snapshotMetaRequest().id(snapshotUri.snapshotId).build(),
                    NETWORK_TIMEOUT
            ).thenApply(response -> {
                PartitionSnapshotMeta snapshotMeta = ((SnapshotMetaResponse) response).meta();

                LOG.info("Copier has loaded the snapshot meta for the partition [{}, meta={}]", createPartitionInfo(), snapshotMeta);

                return snapshotMeta;
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private boolean metadataIsSufficientlyComplete(PartitionSnapshotMeta snapshotMeta) {
        int latestCatalogVersion = partitionSnapshotStorage.catalogService().latestCatalogVersion();

        return snapshotMeta.requiredCatalogVersion() <= latestCatalogVersion;
    }

    private void logMetadataInsufficiencyAndSetError(PartitionSnapshotMeta snapshotMeta) {
        LOG.warn(
                "Metadata not yet available, rejecting snapshot installation [uri={}, requiredVersion={}].",
                this.snapshotUri,
                snapshotMeta.requiredCatalogVersion()
        );

        String errorMessage = String.format(
                "Metadata not yet available, URI '%s', required level %s; rejecting snapshot installation.",
                this.snapshotUri,
                snapshotMeta.requiredCatalogVersion()
        );

        if (isOk()) {
            setError(RaftError.EBUSY, errorMessage);
        }
    }

    /**
     * Requests and stores data into {@link MvPartitionStorage}.
     */
    private CompletableFuture<?> loadSnapshotMvData(SnapshotContext snapshotContext, ClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.messagingService().invoke(
                    snapshotSender,
                    TABLE_MSG_FACTORY.snapshotMvDataRequest()
                            .id(snapshotUri.snapshotId)
                            .batchSizeHint(MAX_MV_DATA_PAYLOADS_BATCH_BYTES_HINT)
                            .build(),
                    NETWORK_TIMEOUT
            ).thenComposeAsync(response -> {
                SnapshotMvDataResponse snapshotMvDataResponse = ((SnapshotMvDataResponse) response);

                for (ResponseEntry entry : snapshotMvDataResponse.rows()) {
                    // Let's write all versions for the row ID.
                    for (int i = 0; i < entry.rowVersions().size(); i++) {
                        if (!busyLock.enterBusy()) {
                            return nullCompletedFuture();
                        }

                        try {
                            writeVersion(snapshotContext, entry, i);
                        } finally {
                            busyLock.leaveBusy();
                        }
                    }
                }

                if (snapshotMvDataResponse.finish()) {
                    LOG.info(
                            "Copier has finished loading multi-versioned data [{}, rows={}]",
                            createPartitionInfo(),
                            snapshotMvDataResponse.rows().size()
                    );

                    return nullCompletedFuture();
                } else {
                    LOG.info(
                            "Copier has loaded a portion of multi-versioned data [{}, rows={}]",
                            createPartitionInfo(),
                            snapshotMvDataResponse.rows().size()
                    );

                    // Let's upload the rest.
                    return loadSnapshotMvData(snapshotContext, snapshotSender);
                }
            }, executor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Requests and stores data into {@link TxStatePartitionStorage}.
     */
    private CompletableFuture<Void> loadSnapshotTxData(ClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.messagingService().invoke(
                    snapshotSender,
                    TABLE_MSG_FACTORY.snapshotTxDataRequest()
                            .id(snapshotUri.snapshotId)
                            .maxTransactionsInBatch(MAX_TX_DATA_BATCH_SIZE)
                            .build(),
                    NETWORK_TIMEOUT
            ).thenComposeAsync(response -> {
                SnapshotTxDataResponse snapshotTxDataResponse = (SnapshotTxDataResponse) response;

                assert snapshotTxDataResponse.txMeta().size() == snapshotTxDataResponse.txIds().size() : createPartitionInfo();

                for (int i = 0; i < snapshotTxDataResponse.txMeta().size(); i++) {
                    if (!busyLock.enterBusy()) {
                        return nullCompletedFuture();
                    }

                    try {
                        partitionSnapshotStorage.txState().addTxMeta(
                                snapshotTxDataResponse.txIds().get(i),
                                snapshotTxDataResponse.txMeta().get(i).asTxMeta()
                        );
                    } finally {
                        busyLock.leaveBusy();
                    }
                }

                if (snapshotTxDataResponse.finish()) {
                    LOG.info(
                            "Copier has finished loading transaction meta [{}, metas={}]",
                            createPartitionInfo(),
                            snapshotTxDataResponse.txMeta().size()
                    );

                    return nullCompletedFuture();
                } else {
                    LOG.info(
                            "Copier has loaded a portion of transaction meta [{}, metas={}]",
                            createPartitionInfo(),
                            snapshotTxDataResponse.txMeta().size()
                    );

                    // Let's upload the rest.
                    return loadSnapshotTxData(snapshotSender);
                }
            }, executor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Completes rebalancing of the partition storages.
     *
     * @param throwable Error occurred while rebalancing the partition storages, {@code null} means that the rebalancing was
     *         successful.
     */
    private CompletableFuture<Void> completeRebalance(SnapshotContext snapshotContext, @Nullable Throwable throwable) {
        if (!busyLock.enterBusy()) {
            if (isOk()) {
                setError(RaftError.ECANCELED, "Copier is cancelled");
            }

            return abortRebalance(snapshotContext);
        }

        try {
            if (throwable != null) {
                String errorMessage = String.format("Partition rebalancing error [%s]", createPartitionInfo());
                partitionSnapshotStorage.failureProcessor().process(new FailureContext(throwable, errorMessage));

                if (isOk()) {
                    setError(RaftError.UNKNOWN, throwable.getMessage());
                }

                return abortRebalance(snapshotContext)
                        .exceptionally(e -> {
                            throwable.addSuppressed(e);

                            return null;
                        })
                        .thenCompose(unused -> failedFuture(throwable));
            }

            if (LOG.isInfoEnabled()) {
                LOG.info("Copier completes the rebalancing of the partition: [{}, meta={}]", createPartitionInfo(), snapshotContext.meta);
            }

            MvPartitionMeta snapshotMeta = mvPartitionMeta(snapshotContext);

            return finishRebalance(snapshotMeta, snapshotContext);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static MvPartitionMeta mvPartitionMeta(SnapshotContext snapshotContext) {
        PartitionSnapshotMeta meta = snapshotContext.meta;

        RaftGroupConfiguration raftGroupConfig = raftGroupConfig(meta);

        LeaseInfo leaseInfo = leaseInfo(meta);

        byte[] raftGroupConfigBytes = VersionedSerialization.toBytes(raftGroupConfig, RaftGroupConfigurationSerializer.INSTANCE);

        var snapshotInfo = new PartitionSnapshotInfo(
                meta.lastIncludedIndex(),
                meta.lastIncludedTerm(),
                leaseInfo,
                raftGroupConfigBytes,
                snapshotContext.partitionsByTableId.keySet()
        );

        byte[] snapshotInfoBytes = VersionedSerialization.toBytes(snapshotInfo, PartitionSnapshotInfoSerializer.INSTANCE);

        return new MvPartitionMeta(
                meta.lastIncludedIndex(),
                meta.lastIncludedTerm(),
                raftGroupConfigBytes,
                leaseInfo,
                snapshotInfoBytes
        );
    }

    private static RaftGroupConfiguration raftGroupConfig(PartitionSnapshotMeta meta) {
        return new RaftGroupConfiguration(
                meta.cfgIndex(),
                meta.cfgTerm(),
                meta.peersList(),
                meta.learnersList(),
                meta.oldPeersList(),
                meta.oldLearnersList()
        );
    }

    private static @Nullable LeaseInfo leaseInfo(PartitionSnapshotMeta meta) {
        if (meta.primaryReplicaNodeId() == null) {
            return null;
        }

        return new LeaseInfo(
                meta.leaseStartTime(),
                meta.primaryReplicaNodeId(),
                meta.primaryReplicaNodeName()
        );
    }

    private int partId() {
        return partitionSnapshotStorage.partitionKey().partitionId();
    }

    private String createPartitionInfo() {
        return partitionSnapshotStorage.partitionKey().toString();
    }

    private void writeVersion(SnapshotContext snapshotContext, ResponseEntry entry, int entryIndex) {
        PartitionMvStorageAccess partition = snapshotContext.partitionsByTableId.get(entry.tableId());

        RowId rowId = new RowId(partId(), entry.rowId());

        BinaryRowMessage rowVersion = entry.rowVersions().get(entryIndex);

        BinaryRow binaryRow = rowVersion == null ? null : rowVersion.asBinaryRow();

        int snapshotCatalogVersion = snapshotContext.meta.requiredCatalogVersion();

        if (entryIndex == entry.timestamps().length) {
            // Writes an intent to write (uncommitted version).
            assert entry.txId() != null;
            assert entry.commitTableOrZoneId() != null;
            assert entry.commitPartitionId() != ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

            partition.addWrite(
                    rowId,
                    binaryRow,
                    entry.txId(),
                    entry.commitTableOrZoneId(),
                    entry.commitPartitionId(),
                    snapshotCatalogVersion
            );
        } else {
            // Writes committed version.
            partition.addWriteCommitted(rowId, binaryRow, hybridTimestamp(entry.timestamps()[entryIndex]), snapshotCatalogVersion);
        }
    }

    private void setNextRowIdToBuildIndexes(SnapshotContext snapshotContext) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            Map<Integer, UUID> nextRowUuidToBuildByIndexId = snapshotContext.meta.nextRowIdToBuildByIndexId();

            if (nullOrEmpty(nextRowUuidToBuildByIndexId)) {
                return;
            }

            Catalog catalog = partitionSnapshotStorage.catalogService().catalog(snapshotContext.meta.requiredCatalogVersion());

            var nextRowIdToBuildByIndexIdAndTableId = new Int2ObjectOpenHashMap<Map<Integer, RowId>>();

            nextRowUuidToBuildByIndexId.forEach((indexId, rowUuid) -> {
                int tableId = catalog.index(indexId).tableId();

                nextRowIdToBuildByIndexIdAndTableId.computeIfAbsent(tableId, unused -> new HashMap<>())
                        .put(indexId, new RowId(partId(), rowUuid));
            });

            for (Int2ObjectMap.Entry<Map<Integer, RowId>> e : nextRowIdToBuildByIndexIdAndTableId.int2ObjectEntrySet()) {
                snapshotContext.partitionsByTableId.get(e.getIntKey()).setNextRowIdToBuildIndex(e.getValue());
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> tryUpdateLowWatermark(SnapshotContext snapshotContext, ClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.messagingService().invoke(
                    snapshotSender,
                    LWM_MSG_FACTORY.getLowWatermarkRequest().build(),
                    NETWORK_TIMEOUT
            ).thenAcceptAsync(response -> {
                GetLowWatermarkResponse getLowWatermarkResponse = (GetLowWatermarkResponse) response;

                HybridTimestamp senderLowWatermark = nullableHybridTimestamp(getLowWatermarkResponse.lowWatermark());

                if (senderLowWatermark != null) {
                    snapshotContext.partitionsByTableId.values()
                            .forEach(mvPartition -> mvPartition.updateLowWatermark(senderLowWatermark));
                }
            }, executor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> startRebalance(SnapshotContext snapshotContext) {
        return allOf(
                aggregateFutureFromPartitions(PartitionMvStorageAccess::startRebalance, snapshotContext),
                partitionSnapshotStorage.txState().startRebalance()
        );
    }

    private CompletableFuture<Void> finishRebalance(MvPartitionMeta meta, SnapshotContext snapshotContext) {
        CompletableFuture<Void> partitionsFinishRebalanceFuture = aggregateFutureFromPartitions(
                mvPartition -> mvPartition.finishRebalance(meta),
                snapshotContext
        );

        // Finish rebalance in tx state after all partitions have finished rebalance in order to guarantee that the snapshot meta
        // information stored in the TX storage consistently reflects the state of the partition storages.
        return partitionsFinishRebalanceFuture.thenComposeAsync(
                v -> partitionSnapshotStorage.txState().finishRebalance(meta),
                executor
        );
    }

    private CompletableFuture<Void> abortRebalance(SnapshotContext snapshotContext) {
        return allOf(
                aggregateFutureFromPartitions(PartitionMvStorageAccess::abortRebalance, snapshotContext),
                partitionSnapshotStorage.txState().abortRebalance()
        );
    }

    private static CompletableFuture<Void> aggregateFutureFromPartitions(
            Function<PartitionMvStorageAccess, CompletableFuture<Void>> action,
            SnapshotContext snapshotContext
    ) {
        CompletableFuture<?>[] futures = snapshotContext.partitionsByTableId.values().stream()
                .map(action)
                .toArray(CompletableFuture[]::new);

        return allOf(futures);
    }

    private static class SnapshotContext {
        final PartitionSnapshotMeta meta;

        final Int2ObjectMap<PartitionMvStorageAccess> partitionsByTableId;

        SnapshotContext(PartitionSnapshotMeta meta, Int2ObjectMap<PartitionMvStorageAccess> partitionsByTableId) {
            this.meta = meta;
            this.partitionsByTableId = partitionsByTableId;
        }
    }
}
