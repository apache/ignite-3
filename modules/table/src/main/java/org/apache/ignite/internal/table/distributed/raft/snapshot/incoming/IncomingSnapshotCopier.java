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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
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

    private static final TableMessagesFactory MSG_FACTORY = new TableMessagesFactory();

    private static final long NETWORK_TIMEOUT = Long.MAX_VALUE;

    private static final long MAX_MV_DATA_PAYLOADS_BATCH_BYTES_HINT = 100 * 1024;

    private static final int MAX_TX_DATA_BATCH_SIZE = 1000;

    private final PartitionSnapshotStorage partitionSnapshotStorage;

    private final SnapshotUri snapshotUri;

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
    private volatile CompletableFuture<?> rebalanceFuture;

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
     */
    public IncomingSnapshotCopier(PartitionSnapshotStorage partitionSnapshotStorage, SnapshotUri snapshotUri) {
        this.partitionSnapshotStorage = partitionSnapshotStorage;
        this.snapshotUri = snapshotUri;
    }

    @Override
    public void start() {
        Executor executor = partitionSnapshotStorage.getIncomingSnapshotsExecutor();

        LOG.info("Copier is started for the partition [{}]", createPartitionInfo());

        rebalanceFuture = partitionSnapshotStorage.partition().startRebalance()
                .thenCompose(unused -> {
                    ClusterNode snapshotSender = getSnapshotSender(snapshotUri.nodeName);

                    if (snapshotSender == null) {
                        throw new StorageRebalanceException("Snapshot sender not found: " + snapshotUri.nodeName);
                    }

                    return loadSnapshotMeta(snapshotSender)
                            .thenCompose(unused1 -> loadSnapshotMvData(snapshotSender, executor))
                            .thenCompose(unused1 -> loadSnapshotTxData(snapshotSender, executor));
                });

        joinFuture = rebalanceFuture.handle((unused, throwable) -> completeRebalance(throwable)).thenCompose(Function.identity());
    }

    @Override
    public void join() throws InterruptedException {
        CompletableFuture<?> fut = joinFuture;

        if (fut != null) {
            try {
                fut.get();
            } catch (CancellationException e) {
                // Ignored.
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                LOG.error("Error when completing the copier", cause);

                if (!isOk()) {
                    setError(RaftError.UNKNOWN, "Unknown error on completion the copier");
                }

                // By analogy with LocalSnapshotCopier#join.
                throw new IllegalStateException(cause);
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

        CompletableFuture<?> fut = rebalanceFuture;

        if (fut != null) {
            fut.cancel(false);

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
            return completedFuture(null);
        }

        try {
            return partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
                    snapshotSender,
                    MSG_FACTORY.snapshotMetaRequest().id(snapshotUri.snapshotId).build(),
                    NETWORK_TIMEOUT
            ).thenAccept(response -> {
                snapshotMeta = ((SnapshotMetaResponse) response).meta();

                LOG.info("Copier has loaded the snapshot meta for the partition [{}, meta={}]", createPartitionInfo(), snapshotMeta);
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Requests and stores data into {@link MvPartitionStorage}.
     */
    private CompletableFuture<?> loadSnapshotMvData(ClusterNode snapshotSender, Executor executor) {
        if (!busyLock.enterBusy()) {
            return completedFuture(null);
        }

        try {
            return partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
                    snapshotSender,
                    MSG_FACTORY.snapshotMvDataRequest()
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
                            return completedFuture(null);
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

                    return completedFuture(null);
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
    private CompletableFuture<?> loadSnapshotTxData(ClusterNode snapshotSender, Executor executor) {
        if (!busyLock.enterBusy()) {
            return completedFuture(null);
        }

        try {
            return partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
                    snapshotSender,
                    MSG_FACTORY.snapshotTxDataRequest()
                            .id(snapshotUri.snapshotId)
                            .maxTransactionsInBatch(MAX_TX_DATA_BATCH_SIZE)
                            .build(),
                    NETWORK_TIMEOUT
            ).thenComposeAsync(response -> {
                SnapshotTxDataResponse snapshotTxDataResponse = (SnapshotTxDataResponse) response;

                assert snapshotTxDataResponse.txMeta().size() == snapshotTxDataResponse.txIds().size() : createPartitionInfo();

                for (int i = 0; i < snapshotTxDataResponse.txMeta().size(); i++) {
                    if (!busyLock.enterBusy()) {
                        return completedFuture(null);
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

                    return completedFuture(null);
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
            if (!isOk()) {
                setError(RaftError.ECANCELED, "Copier is cancelled");
            }

            return partitionSnapshotStorage.partition().abortRebalance();
        }

        try {
            if (throwable != null) {
                LOG.error("Partition rebalancing error [{}]", throwable, createPartitionInfo());

                if (!isOk()) {
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

        ByteBuffer rowVersion = entry.rowVersions().get(i);

        BinaryRow binaryRow = rowVersion == null ? null : new ByteBufferRow(rowVersion.rewind());

        PartitionAccess partition = partitionSnapshotStorage.partition();

        if (i == entry.timestamps().length) {
            // Writes an intent to write (uncommitted version).
            assert entry.txId() != null;
            assert entry.commitTableId() != null;
            assert entry.commitPartitionId() != ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

            partition.addWrite(rowId, binaryRow, entry.txId(), entry.commitTableId(), entry.commitPartitionId());
        } else {
            // Writes committed version.
            partition.addWriteCommitted(rowId, binaryRow, hybridTimestamp(entry.timestamps()[i]));
        }
    }
}
