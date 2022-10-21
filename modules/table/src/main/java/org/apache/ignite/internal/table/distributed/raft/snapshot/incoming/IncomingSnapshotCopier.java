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

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.table.distributed.raft.snapshot.incoming.IncomingSnapshotCopier.SnapshotCopierException.copierException;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.Status;
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

    private static final long NETWORK_TIMEOUT = 10_000;

    private final PartitionSnapshotStorage partitionSnapshotStorage;

    private final SnapshotUri snapshotUri;

    private final IgniteSpinBusyLock cancelBusyLock = new IgniteSpinBusyLock();

    private final CompletableFuture<?> future = new CompletableFuture<>();

    /**
     * Snapshot meta read from the leader.
     *
     * @see SnapshotMetaRequest
     */
    @Nullable
    private volatile SnapshotMeta snapshotMeta;

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
        runAsync(this::start0, partitionSnapshotStorage.getIncomingSnapshotsExecutor());
    }

    @Override
    public void join() throws InterruptedException {
        try {
            future.get();
        } catch (CancellationException e) {
            LOG.info("Copier is cancelled");

            if (!isOk()) {
                setError(RaftError.ECANCELED, "Copier has been cancelled");
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            LOG.error("Error when completing the copier", cause);

            if (cause instanceof SnapshotCopierException) {
                SnapshotCopierException snapshotCopierException = (SnapshotCopierException) cause;

                if (!isOk()) {
                    setError(snapshotCopierException.status.getRaftError(), snapshotCopierException.status.getErrorMsg());
                }

                cause = snapshotCopierException.getCause();
            } else {
                if (!isOk()) {
                    setError(RaftError.UNKNOWN, "Unknown error on completion the copier");
                }
            }

            throw new IllegalStateException(cause);
        }
    }

    @Override
    public void cancel() {
        cancelBusyLock.block();

        future.cancel(true);
    }

    @Override
    public void close() throws IOException {
        cancel();

        try {
            join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public SnapshotReader getReader() {
        // This one's called when "join" is complete.
        return new IncomingSnapshotReader(snapshotMeta);
    }

    private void start0() {
        if (!cancelBusyLock.enterBusy()) {
            return;
        }

        try {
            partitionSnapshotStorage.partition().reCreatePartition()
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            future.completeExceptionally(copierException(throwable, RaftError.EIO, "Error while recreating partition"));
                        } else {
                            ClusterNode snapshotSenderNode = partitionSnapshotStorage.topologyService()
                                    .getByConsistentId(snapshotUri.nodeName);

                            if (snapshotSenderNode == null) {
                                future.completeExceptionally(copierException(
                                        new IgniteException("Sender node was not found or it is offline: " + snapshotUri.nodeName),
                                        RaftError.UNKNOWN,
                                        "Sender node was not found or it is offline"
                                ));
                            } else {
                                requestSnapshotMetaAsync(snapshotSenderNode);
                            }
                        }
                    });
        } catch (StorageException e) {
            future.completeExceptionally(copierException(e, RaftError.EIO, "Error recreating partition"));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        } finally {
            cancelBusyLock.leaveBusy();
        }
    }

    private void requestSnapshotMetaAsync(ClusterNode snapshotSenderNode) {
        if (!cancelBusyLock.enterBusy()) {
            return;
        }

        try {
            partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
                    snapshotSenderNode,
                    MSG_FACTORY.snapshotMetaRequest().id(snapshotUri.snapshotId).build(),
                    NETWORK_TIMEOUT
            ).whenComplete((networkMessage, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(copierException(throwable, RaftError.EIO, "Failed to request snapshot meta"));
                } else {
                    snapshotMeta = ((SnapshotMetaResponse) networkMessage).meta();

                    requestSnapshotMvDataAsync(snapshotSenderNode);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        } finally {
            cancelBusyLock.leaveBusy();
        }
    }

    private void requestSnapshotMvDataAsync(ClusterNode snapshotSenderNode) {
        if (!cancelBusyLock.enterBusy()) {
            return;
        }

        try {
            partitionSnapshotStorage.outgoingSnapshotsManager().messagingService().invoke(
                    snapshotSenderNode,
                    MSG_FACTORY.snapshotMvDataRequest().id(snapshotUri.snapshotId).build(),
                    NETWORK_TIMEOUT
            ).whenComplete((networkMessage, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(copierException(throwable, RaftError.EIO, "Failed to load mv partition data"));
                } else {
                    SnapshotMvDataResponse snapshotMvDataResponse = ((SnapshotMvDataResponse) networkMessage);

                    handleVersionChains(snapshotMvDataResponse.rows());

                    if (!snapshotMvDataResponse.finish()) {
                        requestSnapshotMvDataAsync(snapshotSenderNode);
                    } else {
                        completeCopySnapshot();
                    }
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        } finally {
            cancelBusyLock.leaveBusy();
        }
    }

    private void completeCopySnapshot() {
        if (!cancelBusyLock.enterBusy()) {
            return;
        }

        try {
            SnapshotMeta snapshotMeta0 = snapshotMeta;

            assert snapshotMeta0 != null;

            partitionSnapshotStorage.partition().lastAppliedIndex(snapshotMeta0.lastIncludedIndex());

            future.complete(null);
        } catch (StorageException e) {
            future.completeExceptionally(copierException(e, RaftError.EIO, "Error writing last applied index"));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        } finally {
            cancelBusyLock.leaveBusy();
        }
    }

    private void handleVersionChains(List<ResponseEntry> responseEntries) {
        for (ResponseEntry entry : responseEntries) {
            if (!cancelBusyLock.enterBusy()) {
                return;
            }

            try {
                partitionSnapshotStorage.partition().writeVersionChain(
                        entry.rowId(),
                        entry.rowVersions(),
                        entry.timestamps(),
                        entry.txId(),
                        entry.commitTableId(),
                        entry.commitPartitionId()
                );
            } catch (StorageException e) {
                future.completeExceptionally(copierException(e, RaftError.EIO, "Error writing version chain"));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                cancelBusyLock.leaveBusy();
            }
        }
    }

    /**
     * Exception along with its status for snapshot copier.
     *
     * <p>The class is needed because the copier status is not thread-safe.
     *
     * @see #isOk()
     * @see #setError(RaftError, String, Object...)
     */
    static class SnapshotCopierException extends RuntimeException {
        /** Should be immutable. */
        @IgniteToStringInclude
        private final Status status;

        private SnapshotCopierException(Throwable cause, Status status) {
            super(cause);

            this.status = status;
        }

        @Override
        public String toString() {
            return S.toString(SnapshotCopierException.class, this, "super", super.toString());
        }

        static SnapshotCopierException copierException(Throwable throwable, RaftError raftError, String errorMsg, Object... args) {
            return new SnapshotCopierException(
                    throwable,
                    nullOrEmpty(args) ? new Status(raftError.getNumber(), errorMsg) : new Status(raftError, errorMsg, args)
            );
        }
    }
}
