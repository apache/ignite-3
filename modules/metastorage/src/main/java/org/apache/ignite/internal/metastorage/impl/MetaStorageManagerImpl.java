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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.IgniteUtils.cancelOrConsume;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.CURSOR_CLOSING_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.CURSOR_EXECUTION_ERR;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.exceptions.OperationTimeoutException;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageLearnerListener;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.Status;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.TopologyEventHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * MetaStorage manager.
 *
 * <p>Responsible for:
 * <ul>
 *     <li>Handling cluster init message.</li>
 *     <li>Managing meta storage lifecycle including instantiation meta storage raft group.</li>
 *     <li>Providing corresponding meta storage service proxy interface</li>
 * </ul>
 */
public class MetaStorageManagerImpl implements MetaStorageManager {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageManagerImpl.class);

    /**
     * Special key for the Vault where the applied revision is stored.
     */
    private static final ByteArray APPLIED_REV = ByteArray.fromString("applied_revision");

    private final ClusterService clusterService;

    /** Vault manager in order to commit processed watches with corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Raft manager that is used for metastorage raft group handling. */
    private final RaftManager raftMgr;

    private final ClusterManagementGroupManager cmgMgr;

    /** Meta storage service. */
    private final CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut = new CompletableFuture<>();

    /** Actual storage for the Metastorage. */
    private final KeyValueStorage storage;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    /**
     * Applied revision of the Meta Storage, that is, the most recent revision that has been processed on this node.
     */
    private volatile long appliedRevision;

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param clusterService Cluster network service.
     * @param raftMgr Raft manager.
     * @param storage Storage. This component owns this resource and will manage its lifecycle.
     */
    public MetaStorageManagerImpl(
            VaultManager vaultMgr,
            ClusterService clusterService,
            ClusterManagementGroupManager cmgMgr,
            RaftManager raftMgr,
            KeyValueStorage storage
    ) {
        this.vaultMgr = vaultMgr;
        this.clusterService = clusterService;
        this.raftMgr = raftMgr;
        this.cmgMgr = cmgMgr;
        this.storage = storage;
    }

    private CompletableFuture<MetaStorageServiceImpl> initializeMetaStorage(Set<String> metaStorageNodes) {
        ClusterNode thisNode = clusterService.topologyService().localMember();

        String thisNodeName = thisNode.name();

        CompletableFuture<RaftGroupService> raftServiceFuture;

        try {
            // We need to configure the replication protocol differently whether this node is a synchronous or asynchronous replica.
            if (metaStorageNodes.contains(thisNodeName)) {
                PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(metaStorageNodes);

                Peer localPeer = configuration.peer(thisNodeName);

                assert localPeer != null;

                raftServiceFuture = raftMgr.startRaftGroupNode(
                        new RaftNodeId(MetastorageGroupId.INSTANCE, localPeer),
                        configuration,
                        new MetaStorageListener(storage),
                        new RaftGroupEventsListener() {
                            @Override
                            public void onLeaderElected(long term) {
                                // TODO: add listener to remove learners when they leave Logical Topology
                                //  see https://issues.apache.org/jira/browse/IGNITE-18554

                                registerTopologyEventListener();

                                // Update the configuration immediately in case we missed some updates.
                                addLearners(clusterService.topologyService().allMembers());
                            }

                            @Override
                            public void onNewPeersConfigurationApplied(PeersAndLearners configuration) {
                            }

                            @Override
                            public void onReconfigurationError(Status status, PeersAndLearners configuration, long term) {
                            }
                        }
                );
            } else {
                PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(metaStorageNodes, Set.of(thisNodeName));

                Peer localPeer = configuration.learner(thisNodeName);

                assert localPeer != null;

                raftServiceFuture = raftMgr.startRaftGroupNode(
                        new RaftNodeId(MetastorageGroupId.INSTANCE, localPeer),
                        configuration,
                        new MetaStorageLearnerListener(storage),
                        RaftGroupEventsListener.noopLsnr
                );
            }
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }

        return raftServiceFuture.thenApply(raftService -> new MetaStorageServiceImpl(raftService, thisNode));
    }

    private void registerTopologyEventListener() {
        clusterService.topologyService().addEventHandler(new TopologyEventHandler() {
            @Override
            public void onAppeared(ClusterNode member) {
                addLearners(List.of(member));
            }

            @Override
            public void onDisappeared(ClusterNode member) {
                metaStorageSvcFut.thenAccept(service -> isCurrentNodeLeader(service.raftGroupService())
                        .thenAccept(isLeader -> {
                            if (isLeader) {
                                service.closeCursors(member.id());
                            }
                        }));
            }
        });
    }

    private void addLearners(Collection<ClusterNode> nodes) {
        if (!busyLock.enterBusy()) {
            LOG.info("Skipping Meta Storage configuration update because the node is stopping");

            return;
        }

        try {
            metaStorageSvcFut
                    .thenApply(MetaStorageServiceImpl::raftGroupService)
                    .thenCompose(raftService -> isCurrentNodeLeader(raftService)
                            .thenCompose(isLeader -> isLeader ? addLearners(raftService, nodes) : completedFuture(null)))
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Unable to change peers on topology update", e);
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> addLearners(RaftGroupService raftService, Collection<ClusterNode> nodes) {
        if (!busyLock.enterBusy()) {
            LOG.info("Skipping Meta Storage configuration update because the node is stopping");

            return completedFuture(null);
        }

        try {
            Set<String> peers = raftService.peers().stream()
                    .map(Peer::consistentId)
                    .collect(toSet());

            Set<String> learners = nodes.stream()
                    .map(ClusterNode::name)
                    .filter(name -> !peers.contains(name))
                    .collect(toSet());

            if (learners.isEmpty()) {
                return completedFuture(null);
            }

            if (LOG.isInfoEnabled()) {
                LOG.info("New Meta Storage learners detected: " + learners);
            }

            PeersAndLearners newConfiguration = PeersAndLearners.fromConsistentIds(peers, learners);

            return raftService.addLearners(newConfiguration.learners());
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Boolean> isCurrentNodeLeader(RaftGroupService raftService) {
        String name = clusterService.topologyService().localMember().name();

        return raftService.refreshLeader()
                .thenApply(v -> raftService.leader().consistentId().equals(name));
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        this.appliedRevision = readAppliedRevision().join();

        storage.start();

        cmgMgr.metaStorageNodes()
                .thenCompose(metaStorageNodes -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.failedFuture(new NodeStoppingException());
                    }

                    try {
                        return initializeMetaStorage(metaStorageNodes);
                    } finally {
                        busyLock.leaveBusy();
                    }
                })
                .whenComplete((service, e) -> {
                    if (e != null) {
                        metaStorageSvcFut.completeExceptionally(e);
                    } else {
                        metaStorageSvcFut.complete(service);
                    }
                });
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        cancelOrConsume(metaStorageSvcFut, MetaStorageServiceImpl::close);

        IgniteUtils.closeAll(
                () -> raftMgr.stopRaftNodes(MetastorageGroupId.INSTANCE),
                storage::close
        );
    }

    @Override
    public long appliedRevision() {
        return appliedRevision;
    }

    @Override
    public void registerPrefixWatch(ByteArray key, WatchListener lsnr) {
        storage.watchPrefix(key.bytes(), appliedRevision + 1, lsnr);
    }

    @Override
    public void registerExactWatch(ByteArray key, WatchListener listener) {
        storage.watchExact(key.bytes(), appliedRevision + 1, listener);
    }

    @Override
    public void registerRangeWatch(ByteArray keyFrom, @Nullable ByteArray keyTo, WatchListener listener) {
        storage.watchRange(keyFrom.bytes(), keyTo == null ? null : keyTo.bytes(), appliedRevision + 1, listener);
    }

    @Override
    public void unregisterWatch(WatchListener lsnr) {
        storage.removeWatch(lsnr);
    }

    @Override
    public void deployWatches() throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            // Meta Storage contract states that all updated entries under a particular revision must be stored in the Vault.
            storage.startWatches(this::saveUpdatedEntriesToVault);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key, revUpperBound));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for given keys and the revision upper bound.
     *
     * @see MetaStorageService#getAll(Set, long)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys, revUpperBound));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Inserts or updates an entry with the given key and the given value.
     *
     * @see MetaStorageService#put(ByteArray, byte[])
     */
    public @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, byte[] val) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.put(key, val));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Inserts or updates an entry with the given key and the given value and retrieves a previous entry for the given key.
     *
     * @see MetaStorageService#getAndPut(ByteArray, byte[])
     */
    public @NotNull CompletableFuture<Entry> getAndPut(@NotNull ByteArray key, byte[] val) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndPut(key, val));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Inserts or updates entries with given keys and given values.
     *
     * @see MetaStorageService#putAll(Map)
     */
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.putAll(vals));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Inserts or updates entries with given keys and given values and retrieves a previous entries for given keys.
     *
     * @see MetaStorageService#getAndPutAll(Map)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndPutAll(vals));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Removes an entry for the given key.
     *
     * @see MetaStorageService#remove(ByteArray)
     */
    public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.remove(key));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Removes an entry for the given key.
     *
     * @see MetaStorageService#getAndRemove(ByteArray)
     */
    public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemove(key));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Removes entries for given keys.
     *
     * @see MetaStorageService#removeAll(Set)
     */
    public @NotNull CompletableFuture<Void> removeAll(@NotNull Set<ByteArray> keys) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.removeAll(keys));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Removes entries for given keys and retrieves previous entries.
     *
     * @see MetaStorageService#getAndRemoveAll(Set)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Set<ByteArray> keys) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemoveAll(keys));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Boolean> invoke(Condition cond, Operation success, Operation failure) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Boolean> invoke(Condition cond, Collection<Operation> success, Collection<Operation> failure) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<StatementResult> invoke(If iif) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(iif));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key range in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * @see MetaStorageService#range(ByteArray, ByteArray, long)
     */
    public Cursor<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return new CursorWrapper<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, revUpperBound)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Cursor<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo) throws NodeStoppingException {
        return range(keyFrom, keyTo, false);
    }

    /**
     * Retrieves entries for the given key range in lexicographic order.
     *
     * @see MetaStorageService#range(ByteArray, ByteArray, boolean)
     */
    public Cursor<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, boolean includeTombstones) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return new CursorWrapper<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, includeTombstones)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key range in lexicographic order. Entries will be filtered out by the current applied revision as an
     * upper bound. Applied revision is a revision of the last successful vault update.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). Could be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and applied revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> rangeWithAppliedRevision(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo)
            throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return new CursorWrapper<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, appliedRevision)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Cursor<Entry> prefix(ByteArray keyPrefix) throws NodeStoppingException {
        return prefix(keyPrefix, -1);
    }

    @Override
    public Cursor<Entry> prefix(ByteArray keyPrefix, long revUpperBound) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return new CursorWrapper<>(metaStorageSvcFut.thenApply(svc -> svc.prefix(keyPrefix, revUpperBound)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Compacts meta storage (removes all tombstone entries and old entries except of entries with latest revision).
     *
     * @see MetaStorageService#compact()
     */
    public @NotNull CompletableFuture<Void> compact() {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(MetaStorageService::compact);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns applied revision from the local storage.
     */
    private CompletableFuture<Long> readAppliedRevision() {
        return vaultMgr.get(APPLIED_REV)
                .thenApply(appliedRevision -> appliedRevision == null ? 0L : bytesToLong(appliedRevision.value()));
    }

    /**
     * Saves processed Meta Storage revision and corresponding entries to the Vault.
     */
    private void saveUpdatedEntriesToVault(long revision, Collection<Entry> updatedEntries) {
        assert revision > appliedRevision : "Remote revision " + revision + " is greater than applied revision " + appliedRevision;

        if (!busyLock.enterBusy()) {
            LOG.info("Skipping applying MetaStorage revision because the node is stopping");

            return;
        }

        try {
            Map<ByteArray, byte[]> batch = IgniteUtils.newHashMap(updatedEntries.size() + 1);

            batch.put(APPLIED_REV, longToBytes(revision));

            updatedEntries.forEach(e -> batch.put(new ByteArray(e.key()), e.value()));

            vaultMgr.putAll(batch).join();

            appliedRevision = revision;
        } finally {
            busyLock.leaveBusy();
        }
    }

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.

    /** Cursor wrapper. */
    private final class CursorWrapper<T> implements Cursor<T> {
        /** Inner cursor future. */
        private final CompletableFuture<Cursor<T>> innerCursorFut;

        /**
         * Constructor.
         *
         * @param innerCursorFut Inner cursor future.
         */
        CursorWrapper(CompletableFuture<Cursor<T>> innerCursorFut) {
            this.innerCursorFut = innerCursorFut;
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            get(innerCursorFut.thenAccept(Cursor::close), CURSOR_CLOSING_ERR);
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            if (!busyLock.enterBusy()) {
                return false;
            }

            try {
                return get(innerCursorFut.thenApply(Iterator::hasNext), CURSOR_EXECUTION_ERR);
            } finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override
        public T next() {
            if (!busyLock.enterBusy()) {
                throw new NoSuchElementException("No such element because node is stopping.");
            }

            try {
                return get(innerCursorFut.thenApply(Iterator::next), CURSOR_EXECUTION_ERR);
            } finally {
                busyLock.leaveBusy();
            }
        }

        private <R> R get(CompletableFuture<R> future, int errorCode) {
            try {
                return future.get();
            } catch (ExecutionException e) {
                throw new MetaStorageException(errorCode, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new MetaStorageException(errorCode, e);
            }
        }
    }

    @TestOnly
    CompletableFuture<MetaStorageServiceImpl> metaStorageServiceFuture() {
        return metaStorageSvcFut;
    }
}
