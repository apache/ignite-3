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
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.RaftSnapshotPartitionMeta;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
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

    private final Int2ObjectMap<PartitionStorageAccess> partitionsByTableId;

    private final SnapshotUri snapshotUri;

    private final long waitForMetadataCatchupMs;

    /** Used to make sure that we execute cancellation at most once. */
    private final AtomicBoolean cancellationGuard = new AtomicBoolean();

    /** Busy lock for synchronous rebalance cancellation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    @Nullable
    private volatile CompletableFuture<PartitionSnapshotMeta> snapshotMetaFuture;

    @Nullable
    private volatile CompletableFuture<Void> rebalanceFuture;

    /**
     * Future is to wait in {@link #join()} because it is important for us to wait for the rebalance to finish or abort.
     */
    @Nullable
    private volatile CompletableFuture<?> joinFuture;

    /**
     * Constructor.
     *
     * @param partitionSnapshotStorage Snapshot storage.
     * @param snapshotUri Snapshot URI.
     * @param waitForMetadataCatchupMs How much time to allow for metadata on this node to reach the catalog version required by an
     *         incoming snapshot.
     */
    public IncomingSnapshotCopier(
            PartitionSnapshotStorage partitionSnapshotStorage,
            SnapshotUri snapshotUri,
            long waitForMetadataCatchupMs
    ) {
        this.partitionSnapshotStorage = partitionSnapshotStorage;
        this.partitionsByTableId = partitionSnapshotStorage.partitionsByTableId();
        this.snapshotUri = snapshotUri;
        this.waitForMetadataCatchupMs = waitForMetadataCatchupMs;
    }

    @Override
    public void start() {
        Executor executor = partitionSnapshotStorage.getIncomingSnapshotsExecutor();

        LOG.info("Copier is started for the partition [{}]", createPartitionInfo());

        ClusterNode snapshotSender = getSnapshotSender(snapshotUri.nodeName);

        CompletableFuture<PartitionSnapshotMeta> metadataSufficiencyFuture;

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
                                    return snapshotMeta;
                                }
                            })
                    );
        }

        this.snapshotMetaFuture = metadataSufficiencyFuture;

        CompletableFuture<Void> rebalanceFuture = metadataSufficiencyFuture
                .thenComposeAsync(snapshotMeta -> {
                    if (snapshotMeta == null) {
                        return nullCompletedFuture();
                    }

                    return startRebalance()
                            .thenCompose(unused -> {
                                assert snapshotSender != null : createPartitionInfo();

                                return loadSnapshotMvData(snapshotMeta, snapshotSender, executor)
                                        .thenCompose(unused1 -> loadSnapshotTxData(snapshotSender, executor))
                                        .thenRunAsync(() -> setNextRowIdToBuildIndexes(snapshotMeta), executor);
                            });
                }, executor);

        this.rebalanceFuture = rebalanceFuture;

        joinFuture = metadataSufficiencyFuture.thenCompose(snapshotMeta -> {
            if (snapshotMeta == null) {
                return nullCompletedFuture();
            }

            return rebalanceFuture
                    .handleAsync((unused, throwable) -> completeRebalance(snapshotMeta, throwable), executor)
                    .thenCompose(Function.identity())
                    .thenCompose(unused -> {
                        assert snapshotSender != null : createPartitionInfo();

                        return tryUpdateLowWatermark(snapshotSender, executor);
                    });
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
                    LOG.error("Error when completing the copier", cause);

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
        CompletableFuture<PartitionSnapshotMeta> snapshotMetaFuture = this.snapshotMetaFuture;

        // This one's called when "join" is complete.
        assert snapshotMetaFuture != null && snapshotMetaFuture.isDone();

        // Use 'null' if we failed or were cancelled, this is what JRaft expects.
        PartitionSnapshotMeta meta = snapshotMetaFuture.isCompletedExceptionally() ? null : snapshotMetaFuture.join();

        return new IncomingSnapshotReader(meta);
    }

    private @Nullable ClusterNode getSnapshotSender(String nodeName) {
        return partitionSnapshotStorage.topologyService().getByConsistentId(nodeName);
    }

    /**
     * Requests and the snapshot meta.
     */
    private CompletableFuture<PartitionSnapshotMeta> loadSnapshotMeta(ClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
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
    private CompletableFuture<?> loadSnapshotMvData(PartitionSnapshotMeta snapshotMeta, ClusterNode snapshotSender, Executor executor) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
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
                            writeVersion(snapshotMeta, entry, i);
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
                    return loadSnapshotMvData(snapshotMeta, snapshotSender, executor);
                }
            }, executor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Requests and stores data into {@link TxStatePartitionStorage}.
     */
    private CompletableFuture<Void> loadSnapshotTxData(ClusterNode snapshotSender, Executor executor) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
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
                    return loadSnapshotTxData(snapshotSender, executor);
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
    private CompletableFuture<Void> completeRebalance(PartitionSnapshotMeta meta, @Nullable Throwable throwable) {
        if (!busyLock.enterBusy()) {
            if (isOk()) {
                setError(RaftError.ECANCELED, "Copier is cancelled");
            }

            return abortRebalance();
        }

        try {
            if (throwable != null) {
                LOG.error("Partition rebalancing error [{}]", throwable, createPartitionInfo());

                if (isOk()) {
                    setError(RaftError.UNKNOWN, throwable.getMessage());
                }

                return abortRebalance().thenCompose(unused -> failedFuture(throwable));
            }

            RaftGroupConfiguration raftGroupConfig = new RaftGroupConfiguration(
                    meta.cfgIndex(),
                    meta.cfgTerm(),
                    meta.peersList(),
                    meta.learnersList(),
                    meta.oldPeersList(),
                    meta.oldLearnersList()
            );

            LOG.info(
                    "Copier completes the rebalancing of the partition: [{}, lastAppliedIndex={}, lastAppliedTerm={}, raftGroupConfig={}]",
                    createPartitionInfo(),
                    meta.lastIncludedIndex(),
                    meta.lastIncludedTerm(),
                    raftGroupConfig
            );

            return finishRebalance(RaftSnapshotPartitionMeta.fromSnapshotMeta(meta, raftGroupConfig));
        } finally {
            busyLock.leaveBusy();
        }
    }

    private int partId() {
        return partitionSnapshotStorage.partitionKey().partitionId();
    }

    private String createPartitionInfo() {
        return partitionSnapshotStorage.partitionKey().toString();
    }

    private void writeVersion(PartitionSnapshotMeta snapshotMeta, ResponseEntry entry, int i) {
        PartitionStorageAccess partition = partitionsByTableId.get(entry.tableId());

        RowId rowId = new RowId(partId(), entry.rowId());

        BinaryRowMessage rowVersion = entry.rowVersions().get(i);

        BinaryRow binaryRow = rowVersion == null ? null : rowVersion.asBinaryRow();

        int snapshotCatalogVersion = snapshotMeta.requiredCatalogVersion();

        if (i == entry.timestamps().length) {
            // Writes an intent to write (uncommitted version).
            assert entry.txId() != null;
            assert entry.commitTableId() != null;
            assert entry.commitPartitionId() != ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

            partition.addWrite(rowId, binaryRow, entry.txId(), entry.commitTableId(), entry.commitPartitionId(), snapshotCatalogVersion);
        } else {
            // Writes committed version.
            partition.addWriteCommitted(rowId, binaryRow, hybridTimestamp(entry.timestamps()[i]), snapshotCatalogVersion);
        }
    }

    private void setNextRowIdToBuildIndexes(PartitionSnapshotMeta snapshotMeta) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            Map<Integer, UUID> nextRowUuidToBuildByIndexId = snapshotMeta.nextRowIdToBuildByIndexId();

            if (nullOrEmpty(nextRowUuidToBuildByIndexId)) {
                return;
            }

            Catalog catalog = partitionSnapshotStorage.catalogService().catalog(snapshotMeta.requiredCatalogVersion());

            var nextRowIdToBuildByIndexIdAndTableId = new HashMap<Integer, Map<Integer, RowId>>();

            nextRowUuidToBuildByIndexId.forEach((indexId, rowUuid) -> {
                int tableId = catalog.index(indexId).tableId();

                nextRowIdToBuildByIndexIdAndTableId.computeIfAbsent(tableId, unused -> new HashMap<>())
                        .put(indexId, new RowId(partId(), rowUuid));
            });

            nextRowIdToBuildByIndexIdAndTableId.forEach((tableId, nextRowIdToBuildByIndexId) ->
                    partitionSnapshotStorage.partitionsByTableId().get(tableId).setNextRowIdToBuildIndex(nextRowIdToBuildByIndexId)
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> tryUpdateLowWatermark(ClusterNode snapshotSender, Executor executor) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
                    snapshotSender,
                    LWM_MSG_FACTORY.getLowWatermarkRequest().build(),
                    NETWORK_TIMEOUT
            ).thenAcceptAsync(response -> {
                GetLowWatermarkResponse getLowWatermarkResponse = (GetLowWatermarkResponse) response;

                HybridTimestamp senderLowWatermark = nullableHybridTimestamp(getLowWatermarkResponse.lowWatermark());

                if (senderLowWatermark != null) {
                    partitionsByTableId.values().forEach(mvPartition -> mvPartition.updateLowWatermark(senderLowWatermark));
                }
            }, executor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> startRebalance() {
        return allOf(
                aggregateFutureFromPartitions(PartitionStorageAccess::startRebalance),
                partitionSnapshotStorage.txState().startRebalance()
        );
    }

    private CompletableFuture<Void> finishRebalance(RaftSnapshotPartitionMeta meta) {
        return allOf(
                aggregateFutureFromPartitions(mvPartition -> mvPartition.finishRebalance(meta)),
                partitionSnapshotStorage.txState().finishRebalance(meta)
        );
    }

    private CompletableFuture<Void> abortRebalance() {
        return allOf(
                aggregateFutureFromPartitions(PartitionStorageAccess::abortRebalance),
                partitionSnapshotStorage.txState().abortRebalance()
        );
    }

    private CompletableFuture<Void> aggregateFutureFromPartitions(Function<PartitionStorageAccess, CompletableFuture<Void>> action) {
        CompletableFuture<?>[] futures = partitionsByTableId.values().stream()
                .map(action)
                .toArray(CompletableFuture[]::new);

        return allOf(futures);
    }
}
