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
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.IgniteThrottledLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkResponse;
import org.apache.ignite.internal.lowwatermark.message.LowWatermarkMessagesFactory;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.raft.PartitionSnapshotMeta;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataResponse;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.raft.PartitionSnapshotInfo;
import org.apache.ignite.internal.partition.replicator.raft.PartitionSnapshotInfoSerializer;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.LogStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
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

    private final IgniteThrottledLogger throttledLogger;

    @Nullable
    private volatile CompletableFuture<SnapshotContext> snapshotMetaFuture;

    @Nullable
    private volatile CompletableFuture<Void> rebalanceFuture;

    /**
     * Future is to wait in {@link #join()} because it is important for us to wait for the rebalance to finish or abort.
     */
    @Nullable
    private volatile CompletableFuture<Void> joinFuture;

    private final IncomingSnapshotStats snapshotStats = new IncomingSnapshotStats();

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
        this.throttledLogger = Loggers.toThrottledLogger(LOG, executor);
        this.waitForMetadataCatchupMs = waitForMetadataCatchupMs;
    }

    @Override
    public void start() {
        snapshotStats.onSnapshotInstallationStart();

        if (LOG.isInfoEnabled()) {
            LOG.info("Rebalance is started [snapshotId={}, {}]", snapshotUri.snapshotId, createPartitionInfo());
        }

        InternalClusterNode snapshotSender = getSnapshotSender(snapshotUri.nodeName);

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
        snapshotStats.onWaitingCatalogPhaseStart();

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Waiting for catalog version [snapshotId={}, {}, catalogVersion={}]",
                    snapshotUri.snapshotId,
                    createPartitionInfo(),
                    snapshotMeta.requiredCatalogVersion()
            );
        }

        CompletableFuture<?> metadataReadyFuture = partitionSnapshotStorage.catalogService()
                .catalogReadyFuture(snapshotMeta.requiredCatalogVersion());
        CompletableFuture<?> readinessTimeoutFuture = completeOnMetadataReadinessTimeout();

        return anyOf(metadataReadyFuture, readinessTimeoutFuture)
                .whenComplete((ignored, throwable) -> {
                    snapshotStats.onWaitingCatalogPhaseEnd();

                    if (LOG.isInfoEnabled()) {
                        LOG.info(
                                "Finished waiting for the catalog readiness [snapshotId={}, {}, waitingTime={}ms]",
                                snapshotUri.snapshotId,
                                createPartitionInfo(),
                                snapshotStats.totalWaitingCatalogPhaseDuration()
                        );
                    }
                });
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
                if (!hasCause(e, CancellationException.class, NodeStoppingException.class, RecipientLeftException.class)) {
                    partitionSnapshotStorage.failureProcessor().process(new FailureContext(e, "Error when completing the copier"));

                    if (isOk()) {
                        setError(RaftError.UNKNOWN, "Unknown error on completion the copier");
                    }

                    // By analogy with LocalSnapshotCopier#join.
                    throw new IllegalStateException(e);
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

        if (LOG.isInfoEnabled()) {
            LOG.info("Rebalance is canceled [snapshotId={}, {}]", snapshotUri.snapshotId, createPartitionInfo());
        }

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

    private @Nullable InternalClusterNode getSnapshotSender(String nodeName) {
        return partitionSnapshotStorage.topologyService().getByConsistentId(nodeName);
    }

    /**
     * Requests the snapshot meta.
     */
    private CompletableFuture<PartitionSnapshotMeta> loadSnapshotMeta(InternalClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        snapshotStats.onLoadSnapshotPhaseStart();

        if (LOG.isInfoEnabled()) {
            LOG.info("Start loading snapshot meta [snapshotId={}, {}]", snapshotUri.snapshotId, createPartitionInfo());
        }

        try {
            return partitionSnapshotStorage.messagingService().invoke(
                    snapshotSender,
                    TABLE_MSG_FACTORY.snapshotMetaRequest().id(snapshotUri.snapshotId).build(),
                    NETWORK_TIMEOUT
            ).thenApply(response -> {
                PartitionSnapshotMeta snapshotMeta = ((SnapshotMetaResponse) response).meta();

                snapshotStats.onLoadSnapshotPhaseEnd();

                if (LOG.isInfoEnabled()) {
                    LOG.info(
                            "Snapshot meta has been loaded [snapshotId={}, {}, meta={}, loadingTime={}ms]",
                            snapshotUri.snapshotId,
                            createPartitionInfo(),
                            snapshotMeta,
                            snapshotStats.totalLoadSnapshotPhaseDuration()
                    );
                }

                return snapshotMeta;
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private boolean metadataIsSufficientlyComplete(PartitionSnapshotMeta snapshotMeta) {
        return partitionSnapshotStorage.catalogService().catalogReadyFuture(snapshotMeta.requiredCatalogVersion()).isDone();
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
    private CompletableFuture<?> loadSnapshotMvData(SnapshotContext snapshotContext, InternalClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        snapshotStats.onLoadMvDataPhaseStart();

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Start loading multi-versioned data [snapshotId={}, {}]",
                    snapshotUri.snapshotId,
                    createPartitionInfo()
            );
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

                snapshotStats.onMvBatchProcessing(snapshotMvDataResponse.rows().size());

                if (snapshotMvDataResponse.finish()) {
                    snapshotStats.onLoadMvDataPhaseEnd();

                    if (LOG.isInfoEnabled()) {
                        LOG.info(
                                "Multi-versioned data has been loaded [snapshotId={}, {}, totalRows={}, totalBatches={},"
                                        + " mvDataLoadingTime={}ms]",
                                snapshotUri.snapshotId,
                                createPartitionInfo(),
                                snapshotMvDataResponse.rows().size(),
                                snapshotStats.totalMvDataRows(),
                                snapshotStats.totalMvDataBatches(),
                                snapshotStats.loadMvDataPhaseDuration()
                        );
                    }

                    return nullCompletedFuture();
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "A portion of multi-versioned data has been loaded [snapshotId={}, {}, rows={}]",
                                snapshotUri.snapshotId,
                                createPartitionInfo(),
                                snapshotMvDataResponse.rows().size()
                        );
                    }

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
    private CompletableFuture<Void> loadSnapshotTxData(InternalClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        snapshotStats.onLoadTxMetasPhaseStart();

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Start loading transaction meta data [snapshotId={}, {}]",
                    snapshotUri.snapshotId,
                    createPartitionInfo()
            );
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

                snapshotStats.onTxMetasBatchProcessing(snapshotTxDataResponse.txMeta().size());

                if (snapshotTxDataResponse.finish()) {
                    snapshotStats.onLoadTxMetasPhaseEnd();

                    if (LOG.isInfoEnabled()) {
                        LOG.info(
                                "Transaction meta has been loaded [snapshotId={}, {}, totalMetas={}, totalBatches={},"
                                        + " metaLoadingTime={}ms]",
                                snapshotUri.snapshotId,
                                createPartitionInfo(),
                                snapshotTxDataResponse.txMeta().size(),
                                snapshotStats.totalTxMetas(),
                                snapshotStats.totalTxMetasBatches(),
                                snapshotStats.loadTxMetasPhaseDuration()
                        );
                    }

                    return nullCompletedFuture();
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "A portion of transaction meta has been loaded [snapshotId={}, {}, metas={}]",
                                snapshotUri.snapshotId,
                                createPartitionInfo(),
                                snapshotTxDataResponse.txMeta().size()
                        );
                    }

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
        snapshotStats.onSnapshotInstallationEnd();

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
                LOG.info(
                        "Rebalance is done [{}, meta={}, rebalanceTime={}ms]",
                        createPartitionInfo(),
                        snapshotContext.meta,
                        snapshotStats.totalSnapshotInstallationDuration()
                );
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
                meta.sequenceToken(),
                meta.oldSequenceToken(),
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
        ZonePartitionKey partitionKey = partitionSnapshotStorage.partitionKey();

        return "zoneId=" + partitionKey.zoneId() + ", partitionId=" + partitionKey.partitionId();
    }

    private void writeVersion(SnapshotContext snapshotContext, ResponseEntry entry, int entryIndex) {
        PartitionMvStorageAccess partition = snapshotContext.partitionsByTableId.get(entry.tableId());

        if (partition == null) {
            // Table might have been removed locally which is a normal situation, we log it just in case.
            throttledLogger.warn("No partition storage found locally for tableId={} while installing a snapshot", entry.tableId());

            return;
        }

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

        snapshotStats.onSetRowIdToBuildPhaseStart();

        try {
            Map<Integer, UUID> nextRowUuidToBuildByIndexId = snapshotContext.meta.nextRowIdToBuildByIndexId();

            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Setting next row ID for index building [snapshotId={}, {}, indexIdToRowId={}]",
                        snapshotUri.snapshotId,
                        createPartitionInfo(),
                        nextRowUuidToBuildByIndexId
                );
            }

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
                int tableId = e.getIntKey();

                PartitionMvStorageAccess partitionAccess = snapshotContext.partitionsByTableId.get(tableId);

                if (partitionAccess == null) {
                    // Table might have been removed locally which is a normal situation, we log it just in case.
                    throttledLogger.warn("No partition storage found locally for tableId={} while installing a snapshot", tableId);
                } else {
                    partitionAccess.setNextRowIdToBuildIndex(e.getValue());
                }
            }

            snapshotStats.onSetRowIdToBuildPhaseEnd();

            if (LOG.isInfoEnabled()) {
                LOG.info("Finished setting next row ID for index building [snapshotId={}, {}, totalTime={}ms]",
                        snapshotUri.snapshotId,
                        createPartitionInfo(),
                        snapshotStats.totalSetRowIdToBuildPhaseDuration()
                );
            }

        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> tryUpdateLowWatermark(SnapshotContext snapshotContext, InternalClusterNode snapshotSender) {
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
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        snapshotStats.onPreparingStoragePhaseStart();

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Preparing storages for snapshot installation [snapshotId={}, {}]",
                    snapshotUri.snapshotId,
                    createPartitionInfo()
            );
        }

        try {
            return allOf(
                    aggregateFutureFromPartitions(PartitionMvStorageAccess::startRebalance, snapshotContext),
                    partitionSnapshotStorage.txState().startRebalance()
            ).thenComposeAsync(unused -> startRebalanceForReplicationLogStorages(snapshotContext), executor)
                    .whenComplete((ignore, throwable) -> {
                        snapshotStats.onPreparingStoragePhaseEnd();

                        if (LOG.isInfoEnabled()) {
                            LOG.info(
                                    "Storages are prepared to load data [snapshotId={}, {}, preparationTime={}ms]",
                                    snapshotUri.snapshotId,
                                    createPartitionInfo(),
                                    snapshotStats.totalPreparingStoragePhaseDuration()
                            );
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
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

    private CompletableFuture<Void> startRebalanceForReplicationLogStorages(SnapshotContext snapshotContext) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            Set<ReplicationLogStorageKey> keys = collectReplicationLogStorageKeys(snapshotContext);

            return runAsync(() -> inBusyLockSafe(busyLock, () -> keys.forEach(this::startRebalanceForReplicationLogStorage)), executor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private Set<ReplicationLogStorageKey> collectReplicationLogStorageKeys(SnapshotContext snapshotContext) {
        return snapshotContext.partitionsByTableId.values().stream()
                .map(partitionMvStorage -> ReplicationLogStorageKey.create(partitionSnapshotStorage, partitionMvStorage))
                .collect(toSet());
    }

    private void startRebalanceForReplicationLogStorage(ReplicationLogStorageKey key) throws IgniteInternalException {
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info("Start rebalance for the replication log storage [snapshotId={}, {}]", snapshotUri.snapshotId, key);
            }

            LogStorageAccess logStorage = partitionSnapshotStorage.logStorage();

            logStorage.destroy(key.replicationGroupId(), key.isVolatile());
            logStorage.createMetaStorage(key.replicationGroupId());
        } catch (NodeStoppingException e) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, e);
        }
    }
}
