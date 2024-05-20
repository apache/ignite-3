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

package org.apache.ignite.internal.table.distributed.raft.snapshot.incoming;

import static java.util.concurrent.CompletableFuture.anyOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;
import static org.apache.ignite.internal.table.distributed.schema.CatalogVersionSufficiency.isMetadataAvailableFor;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkResponse;
import org.apache.ignite.internal.lowwatermark.message.LowWatermarkMessagesFactory;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataResponse;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryRowMessage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot copier implementation for partitions. Used to stream partition data from the leader to the local node.
 */
public class IncomingSnapshotCopier extends SnapshotCopier {
    private static final IgniteLogger LOG = Loggers.forClass(IncomingSnapshotCopier.class);

    private static final TableMessagesFactory TABLE_MSG_FACTORY = new TableMessagesFactory();

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

    /**
     * Snapshot meta read from the leader.
     *
     * @see SnapshotMetaRequest
     */
    @Nullable
    private volatile SnapshotMeta snapshotMeta;

    @Nullable
    private volatile CompletableFuture<Boolean> metadataSufficiencyFuture;

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
     * @param waitForMetadataCatchupMs How much time to allow for metadata on this node to reach the catalog version required by
     *     an incoming snapshot.
     */
    public IncomingSnapshotCopier(
            PartitionSnapshotStorage partitionSnapshotStorage,
            SnapshotUri snapshotUri,
            long waitForMetadataCatchupMs
    ) {
        this.partitionSnapshotStorage = partitionSnapshotStorage;
        this.snapshotUri = snapshotUri;
        this.waitForMetadataCatchupMs = waitForMetadataCatchupMs;
    }

    @Override
    public void start() {
        Executor executor = partitionSnapshotStorage.getIncomingSnapshotsExecutor();

        LOG.info("Copier is started for the partition [{}]", createPartitionInfo());

        ClusterNode snapshotSender = getSnapshotSender(snapshotUri.nodeName);

        metadataSufficiencyFuture = snapshotSender == null
                ? failedFuture(new StorageRebalanceException("Snapshot sender not found: " + snapshotUri.nodeName))
                : loadSnapshotMeta(snapshotSender)
                        // Give metadata some time to catch up as it's very probable that the leader is ahead metadata-wise.
                        .thenCompose(unused -> waitForMetadataWithTimeout())
                        .thenApply(unused -> {
                            boolean metadataIsSufficientlyComplete = metadataIsSufficientlyComplete();

                            if (!metadataIsSufficientlyComplete) {
                                logMetadataInsufficiencyAndSetError();
                            }

                            return metadataIsSufficientlyComplete;
                        })
                ;

        rebalanceFuture = metadataSufficiencyFuture.thenComposeAsync(metadataSufficient -> {
            if (metadataSufficient) {
                return partitionSnapshotStorage.partition().startRebalance()
                        .thenCompose(unused -> {
                            assert snapshotSender != null : createPartitionInfo();

                            return loadSnapshotMvData(snapshotSender, executor)
                                    .thenCompose(unused1 -> loadSnapshotTxData(snapshotSender, executor))
                                    .thenRunAsync(this::setNextRowIdToBuildIndexes, executor);
                        });
            } else {
                return nullCompletedFuture();
            }
        }, executor);

        joinFuture = metadataSufficiencyFuture.thenCompose(metadataSufficient -> {
            if (metadataSufficient) {
                return rebalanceFuture.handleAsync((unused, throwable) -> completeRebalance(throwable), executor)
                        .thenCompose(Function.identity())
                        .thenCompose(unused -> {
                            assert snapshotSender != null : createPartitionInfo();

                            return tryUpdateLowWatermark(snapshotSender, executor);
                        });
            } else {
                return nullCompletedFuture();
            }
        });
    }

    private CompletableFuture<?> waitForMetadataWithTimeout() {
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
        List<CompletableFuture<?>> futuresToCancel = Stream.of(metadataSufficiencyFuture, rebalanceFuture)
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
        // This one's called when "join" is complete.
        return new IncomingSnapshotReader(snapshotMeta);
    }

    private @Nullable ClusterNode getSnapshotSender(String nodeName) {
        return partitionSnapshotStorage.topologyService().getByConsistentId(nodeName);
    }

    /**
     * Requests and saves the snapshot meta in {@link #snapshotMeta}.
     */
    private CompletableFuture<?> loadSnapshotMeta(ClusterNode snapshotSender) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
                    snapshotSender,
                    TABLE_MSG_FACTORY.snapshotMetaRequest().id(snapshotUri.snapshotId).build(),
                    NETWORK_TIMEOUT
            ).thenAccept(response -> {
                snapshotMeta = ((SnapshotMetaResponse) response).meta();

                LOG.info("Copier has loaded the snapshot meta for the partition [{}, meta={}]", createPartitionInfo(), snapshotMeta);
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private boolean metadataIsSufficientlyComplete() {
        SnapshotMeta meta = snapshotMeta;
        assert meta != null;

        return isMetadataAvailableFor(meta.requiredCatalogVersion(), partitionSnapshotStorage.catalogService());
    }

    private void logMetadataInsufficiencyAndSetError() {
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
    private CompletableFuture<?> loadSnapshotMvData(ClusterNode snapshotSender, Executor executor) {
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
                            writeVersion(entry, i);
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
                    return loadSnapshotMvData(snapshotSender, executor);
                }
            }, executor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Requests and stores data into {@link TxStateStorage}.
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
                        partitionSnapshotStorage.partition().addTxMeta(
                                snapshotTxDataResponse.txIds().get(i),
                                snapshotTxDataResponse.txMeta().get(i)
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
     * @param throwable Error occurred while rebalancing the partition storages, {@code null} means that the rebalancing was successful.
     */
    private CompletableFuture<Void> completeRebalance(@Nullable Throwable throwable) {
        if (!busyLock.enterBusy()) {
            if (isOk()) {
                setError(RaftError.ECANCELED, "Copier is cancelled");
            }

            return partitionSnapshotStorage.partition().abortRebalance();
        }

        try {
            if (throwable != null) {
                LOG.error("Partition rebalancing error [{}]", throwable, createPartitionInfo());

                if (isOk()) {
                    setError(RaftError.UNKNOWN, throwable.getMessage());
                }

                return partitionSnapshotStorage.partition().abortRebalance().thenCompose(unused -> failedFuture(throwable));
            }

            SnapshotMeta meta = snapshotMeta;

            RaftGroupConfiguration raftGroupConfig = new RaftGroupConfiguration(
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

            return partitionSnapshotStorage.partition().finishRebalance(meta.lastIncludedIndex(), meta.lastIncludedTerm(), raftGroupConfig);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private int partId() {
        return partitionSnapshotStorage.partition().partitionKey().partitionId();
    }

    private String createPartitionInfo() {
        return "tableId=" + partitionSnapshotStorage.partition().partitionKey().tableId() + ", partitionId=" + partId();
    }

    private void writeVersion(ResponseEntry entry, int i) {
        RowId rowId = new RowId(partId(), entry.rowId());

        BinaryRowMessage rowVersion = entry.rowVersions().get(i);

        BinaryRow binaryRow = rowVersion == null ? null : rowVersion.asBinaryRow();

        PartitionAccess partition = partitionSnapshotStorage.partition();

        int snapshotCatalogVersion = snapshotMeta.requiredCatalogVersion();

        if (i == entry.timestamps().length) {
            // Writes an intent to write (uncommitted version).
            assert entry.txId() != null;
            assert entry.commitTableId() != null;
            assert entry.commitPartitionId() != ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

            partition.addWrite(rowId, binaryRow, entry.txId(), entry.commitZoneId(),
                    entry.commitTableId(), entry.commitPartitionId(), snapshotCatalogVersion);
        } else {
            // Writes committed version.
            partition.addWriteCommitted(rowId, binaryRow, hybridTimestamp(entry.timestamps()[i]), snapshotCatalogVersion);
        }
    }

    private void setNextRowIdToBuildIndexes() {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            Map<Integer, UUID> nextRowIdToBuildByIndexId = snapshotMeta.nextRowIdToBuildByIndexId();

            if (!nullOrEmpty(nextRowIdToBuildByIndexId)) {
                Map<Integer, RowId> nextRowIdToBuildByIndexId0 = nextRowIdToBuildByIndexId.entrySet().stream()
                        .collect(toMap(Entry::getKey, e -> new RowId(partId(), e.getValue())));

                partitionSnapshotStorage.partition().setNextRowIdToBuildIndex(nextRowIdToBuildByIndexId0);
            }
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
                    partitionSnapshotStorage.partition().updateLowWatermark(senderLowWatermark);
                }
            }, executor);
        } finally {
            busyLock.leaveBusy();
        }
    }
}
