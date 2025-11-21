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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotRequestMessage;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataRequest;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Outgoing snapshots manager. Manages a collection of all outgoing snapshots, currently present on the Ignite node.
 */
public class OutgoingSnapshotsManager implements PartitionsSnapshots, IgniteComponent {
    /**
     * Logger.
     */
    private static final IgniteLogger LOG = Loggers.forClass(OutgoingSnapshotsManager.class);

    private final String nodeName;

    /**
     * Messaging service.
     */
    private final MessagingService messagingService;

    private final FailureProcessor failureProcessor;

    /**
     * Map with outgoing snapshots.
     */
    private final Map<UUID, OutgoingSnapshot> snapshots = new ConcurrentHashMap<>();
    private final Map<PartitionKey, PartitionSnapshotsImpl> snapshotsByPartition = new ConcurrentHashMap<>();

    private volatile ExecutorService executor;

    /**
     * Constructor.
     *
     * @param messagingService Messaging service.
     */
    public OutgoingSnapshotsManager(String nodeName, MessagingService messagingService, FailureProcessor failureProcessor) {
        this.nodeName = nodeName;
        this.messagingService = messagingService;
        this.failureProcessor = failureProcessor;
    }

    /**
     * Returns a messaging service.
     */
    public MessagingService messagingService() {
        return messagingService;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                4, 4,
                10, SECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "outgoing-snapshots", LOG, STORAGE_READ)
        );
        threadPoolExecutor.allowCoreThreadTimeOut(true);

        executor = threadPoolExecutor;

        messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        // At this moment, all RAFT groups should already be stopped, so all snapshots are already closed and finished.

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, SECONDS);

        return nullCompletedFuture();
    }

    /**
     * Starts an outgoing snapshot and registers it in the manager. This is the point where snapshot is 'taken', that is, the immutable
     * scope of the snapshot (what MV data and what TX data belongs to it) is established.
     *
     * @param snapshotId Snapshot id.
     * @param outgoingSnapshot Outgoing snapshot.
     */
    void startOutgoingSnapshot(UUID snapshotId, OutgoingSnapshot outgoingSnapshot) {
        snapshots.put(snapshotId, outgoingSnapshot);

        PartitionSnapshotsImpl partitionSnapshots = getPartitionSnapshots(outgoingSnapshot.partitionKey());

        partitionSnapshots.freezeAndAddUnderLock(outgoingSnapshot);
    }

    private PartitionSnapshotsImpl getPartitionSnapshots(PartitionKey partitionKey) {
        return snapshotsByPartition.computeIfAbsent(
                partitionKey,
                key -> new PartitionSnapshotsImpl()
        );
    }

    /**
     * Removes an outgoing snapshot from the manager.
     *
     * @param snapshotId Snapshot id.
     */
    @Override
    public void finishOutgoingSnapshot(UUID snapshotId) {
        OutgoingSnapshot removedSnapshot = snapshots.remove(snapshotId);

        if (removedSnapshot != null) {
            PartitionSnapshotsImpl partitionSnapshots = getPartitionSnapshots(removedSnapshot.partitionKey());

            partitionSnapshots.removeUnderLock(removedSnapshot);

            removedSnapshot.close();
        }
    }

    private void handleMessage(NetworkMessage networkMessage, InternalClusterNode sender, @Nullable Long correlationId) {
        // Ignore all messages that we can't handle.
        if (!(networkMessage instanceof SnapshotRequestMessage)) {
            return;
        }

        assert correlationId != null;

        OutgoingSnapshot outgoingSnapshot = snapshots.get(((SnapshotRequestMessage) networkMessage).id());

        if (outgoingSnapshot == null) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Unexpected snapshot request message has been received [message={}]", networkMessage);
            }

            return;
        }

        supplyAsync(() -> handleSnapshotRequestMessage(networkMessage, outgoingSnapshot), executor)
                .whenCompleteAsync((response, throwable) -> respond(response, throwable, sender, correlationId), executor);
    }

    private static @Nullable NetworkMessage handleSnapshotRequestMessage(NetworkMessage networkMessage, OutgoingSnapshot outgoingSnapshot) {
        switch (networkMessage.messageType()) {
            case PartitionReplicationMessageGroup.SNAPSHOT_META_REQUEST:
                return outgoingSnapshot.handleSnapshotMetaRequest((SnapshotMetaRequest) networkMessage);

            case PartitionReplicationMessageGroup.SNAPSHOT_MV_DATA_REQUEST:
                return outgoingSnapshot.handleSnapshotMvDataRequest((SnapshotMvDataRequest) networkMessage);

            case PartitionReplicationMessageGroup.SNAPSHOT_TX_DATA_REQUEST:
                return outgoingSnapshot.handleSnapshotTxDataRequest((SnapshotTxDataRequest) networkMessage);

            default:
                return null;
        }
    }

    private void respond(@Nullable NetworkMessage response, @Nullable Throwable throwable, InternalClusterNode sender, long correlationId) {
        if (throwable != null) {
            if (!hasCause(throwable, NodeStoppingException.class, StorageClosedException.class)) {
                failureProcessor.process(new FailureContext(throwable, "Something went wrong while handling a request"));
            }

            return;
        }

        // Can happen on node stop, see "OutgoingSnapshot#logThatAlreadyClosedAndReturnNull".
        if (response == null) {
            return;
        }

        messagingService.respond(sender, response, correlationId)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Could not send a response with correlationId={}", e, correlationId);
                    }
                });
    }

    @Override
    public PartitionSnapshots partitionSnapshots(PartitionKey partitionKey) {
        return getPartitionSnapshots(partitionKey);
    }

    @Override
    public void cleanupOutgoingSnapshots(PartitionKey partitionKey) {
        PartitionSnapshots partitionSnapshots = snapshotsByPartition.remove(partitionKey);

        if (partitionSnapshots == null) {
            return;
        }

        partitionSnapshots.acquireReadLock();

        try {
            partitionSnapshots.ongoingSnapshots().forEach(snapshot -> finishOutgoingSnapshot(snapshot.id()));
        } finally {
            partitionSnapshots.releaseReadLock();
        }
    }

    private static class PartitionSnapshotsImpl implements PartitionSnapshots {
        private final List<OutgoingSnapshot> snapshots = new ArrayList<>();

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        private void freezeAndAddUnderLock(OutgoingSnapshot snapshot) {
            lock.writeLock().lock();

            try {
                // Cut consistent view of TX data and take snapshot metadata.
                snapshot.freezeScopeUnderMvLock();

                // Install the snapshot in the collection of snapshots on this partition, effectively establishing
                // a consistent view over MV data.
                snapshots.add(snapshot);
            } finally {
                lock.writeLock().unlock();
            }
        }

        private void removeUnderLock(OutgoingSnapshot snapshot) {
            lock.writeLock().lock();

            try {
                snapshots.remove(snapshot);
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void acquireReadLock() {
            lock.readLock().lock();
        }

        @Override
        public void releaseReadLock() {
            lock.readLock().unlock();
        }

        @Override
        public List<OutgoingSnapshot> ongoingSnapshots() {
            assert lock.getReadHoldCount() > 0 : "Current thread does not hold the read lock";

            return unmodifiableList(snapshots);
        }
    }
}
