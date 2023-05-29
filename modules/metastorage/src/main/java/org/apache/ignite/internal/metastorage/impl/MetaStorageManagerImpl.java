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
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.IgniteUtils.cancelOrConsume;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.RESTORING_STORAGE_ERR;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeDisruptorConfiguration;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * MetaStorage manager.
 *
 * <p>Responsible for:
 * <ul>
 *     <li>Handling cluster init message.</li>
 *     <li>Managing Meta storage lifecycle including instantiation Meta storage raft group.</li>
 *     <li>Providing corresponding Meta storage service proxy interface</li>
 * </ul>
 */
public class MetaStorageManagerImpl implements MetaStorageManager {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageManagerImpl.class);

    /**
     * Special key for the Vault where the applied revision is stored.
     */
    private static final ByteArray APPLIED_REV_KEY = new ByteArray("applied_revision");

    private final ClusterService clusterService;

    /** Vault manager in order to commit processed watches with corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Raft manager that is used for metastorage raft group handling. */
    private final RaftManager raftMgr;

    private final ClusterManagementGroupManager cmgMgr;

    private final LogicalTopologyService logicalTopologyService;

    /** Meta storage service. */
    private final CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut = new CompletableFuture<>();

    /** Actual storage for Meta storage. */
    private final KeyValueStorage storage;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    private final ClusterTimeImpl clusterTime;

    private volatile long appliedRevision;

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param clusterService Cluster network service.
     * @param cmgMgr Cluster management service Manager.
     * @param logicalTopologyService Logical topology service.
     * @param raftMgr Raft manager.
     * @param storage Storage. This component owns this resource and will manage its lifecycle.
     * @param clock A hybrid logical clock.
     */
    public MetaStorageManagerImpl(
            VaultManager vaultMgr,
            ClusterService clusterService,
            ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftMgr,
            KeyValueStorage storage,
            HybridClock clock
    ) {
        this.vaultMgr = vaultMgr;
        this.clusterService = clusterService;
        this.raftMgr = raftMgr;
        this.cmgMgr = cmgMgr;
        this.logicalTopologyService = logicalTopologyService;
        this.storage = storage;
        this.clusterTime = new ClusterTimeImpl(busyLock, clock);
    }

    private CompletableFuture<MetaStorageServiceImpl> initializeMetaStorage(Set<String> metaStorageNodes) {
        String thisNodeName = clusterService.nodeName();

        CompletableFuture<RaftGroupService> raftServiceFuture;

        try {
            var ownFsmCallerExecutorDisruptorConfig = new RaftNodeDisruptorConfiguration("metastorage", 1);

            // We need to configure the replication protocol differently whether this node is a synchronous or asynchronous replica.
            if (metaStorageNodes.contains(thisNodeName)) {
                PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(metaStorageNodes);

                Peer localPeer = configuration.peer(thisNodeName);

                assert localPeer != null;

                raftServiceFuture = raftMgr.startRaftGroupNodeAndWaitNodeReadyFuture(
                        new RaftNodeId(MetastorageGroupId.INSTANCE, localPeer),
                        configuration,
                        new MetaStorageListener(storage, clusterTime),
                        new MetaStorageRaftGroupEventsListener(
                                busyLock,
                                clusterService,
                                logicalTopologyService,
                                metaStorageSvcFut,
                                clusterTime
                        ),
                        ownFsmCallerExecutorDisruptorConfig
                );
            } else {
                PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(metaStorageNodes, Set.of(thisNodeName));

                Peer localPeer = configuration.learner(thisNodeName);

                assert localPeer != null;

                raftServiceFuture = raftMgr.startRaftGroupNodeAndWaitNodeReadyFuture(
                        new RaftNodeId(MetastorageGroupId.INSTANCE, localPeer),
                        configuration,
                        new MetaStorageListener(storage, clusterTime),
                        RaftGroupEventsListener.noopLsnr,
                        ownFsmCallerExecutorDisruptorConfig
                );
            }
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }

        return raftServiceFuture.thenApply(raftService -> new MetaStorageServiceImpl(thisNodeName, raftService, busyLock, clusterTime));
    }

    @Override
    public void start() {
        storage.start();

        appliedRevision = readRevisionFromVault();

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

    private long readRevisionFromVault() {
        try {
            VaultEntry entry = vaultMgr.get(APPLIED_REV_KEY).get(10, TimeUnit.SECONDS);

            return entry == null ? 0L : bytesToLong(entry.value());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new MetaStorageException(RESTORING_STORAGE_ERR, e);
        }
    }

    @Override
    public void stop() throws Exception {
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        clusterTime.stopLeaderTimer();

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
    public void registerPrefixWatch(ByteArray key, WatchListener listener) {
        storage.watchRange(key.bytes(), storage.nextKey(key.bytes()), appliedRevision() + 1, listener);
    }

    @Override
    public void registerExactWatch(ByteArray key, WatchListener listener) {
        storage.watchExact(key.bytes(), appliedRevision() + 1, listener);
    }

    @Override
    public void registerRangeWatch(ByteArray keyFrom, @Nullable ByteArray keyTo, WatchListener listener) {
        storage.watchRange(keyFrom.bytes(), keyTo == null ? null : keyTo.bytes(), appliedRevision() + 1, listener);
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
            storage.startWatches(this::onRevisionApplied);
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
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
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
    @Override
    public CompletableFuture<Void> put(ByteArray key, byte[] val) {
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
    public CompletableFuture<Entry> getAndPut(ByteArray key, byte[] val) {
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
    public CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals) {
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
    public CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(Map<ByteArray, byte[]> vals) {
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
    public CompletableFuture<Void> remove(ByteArray key) {
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
    public CompletableFuture<Entry> getAndRemove(ByteArray key) {
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
    public CompletableFuture<Void> removeAll(Set<ByteArray> keys) {
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
    public CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(Set<ByteArray> keys) {
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
    public CompletableFuture<StatementResult> invoke(Iif iif) {
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
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return new NodeStoppingPublisher<>();
        }

        try {
            return new CompletableFuturePublisher<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, revUpperBound)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return range(keyFrom, keyTo, false);
    }

    /**
     * Retrieves entries for the given key range in lexicographic order.
     *
     * @see MetaStorageService#range(ByteArray, ByteArray, boolean)
     */
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, boolean includeTombstones) {
        if (!busyLock.enterBusy()) {
            return new NodeStoppingPublisher<>();
        }

        try {
            return new CompletableFuturePublisher<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, includeTombstones)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Publisher<Entry> prefix(ByteArray keyPrefix) {
        return prefix(keyPrefix, MetaStorageManager.LATEST_REVISION);
    }

    @Override
    public Publisher<Entry> prefix(ByteArray keyPrefix, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return new NodeStoppingPublisher<>();
        }

        try {
            return new CompletableFuturePublisher<>(metaStorageSvcFut.thenApply(svc -> svc.prefix(keyPrefix, revUpperBound)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Compacts Meta storage (removes all tombstone entries and old entries except of entries with latest revision).
     *
     * @see MetaStorageService#compact()
     */
    public CompletableFuture<Void> compact() {
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
     * Saves processed Meta Storage revision and corresponding entries to the Vault.
     */
    private CompletableFuture<Void> onRevisionApplied(WatchEvent watchEvent, HybridTimestamp time) {
        assert time != null;

        if (!busyLock.enterBusy()) {
            LOG.info("Skipping applying MetaStorage revision because the node is stopping");

            return completedFuture(null);
        }

        clusterTime.updateSafeTime(time);

        try {
            CompletableFuture<Void> saveToVaultFuture;

            if (watchEvent.entryEvents().isEmpty()) {
                saveToVaultFuture = vaultMgr.put(APPLIED_REV_KEY, longToBytes(watchEvent.revision()));
            } else {
                Map<ByteArray, byte[]> batch = IgniteUtils.newHashMap(watchEvent.entryEvents().size() + 1);

                batch.put(APPLIED_REV_KEY, longToBytes(watchEvent.revision()));

                watchEvent.entryEvents().forEach(e -> batch.put(new ByteArray(e.newEntry().key()), e.newEntry().value()));

                saveToVaultFuture = vaultMgr.putAll(batch);
            }

            return saveToVaultFuture.thenRun(() -> appliedRevision = watchEvent.revision());
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public ClusterTime clusterTime() {
        return clusterTime;
    }

    @TestOnly
    CompletableFuture<MetaStorageServiceImpl> metaStorageServiceFuture() {
        return metaStorageSvcFut;
    }

    private static class CompletableFuturePublisher<T> implements Publisher<T> {
        private final CompletableFuture<Publisher<T>> future;

        CompletableFuturePublisher(CompletableFuture<Publisher<T>> future) {
            this.future = future;
        }

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            future.whenComplete((publisher, e) -> {
                if (e != null) {
                    subscriber.onError(e);
                } else {
                    publisher.subscribe(subscriber);
                }
            });
        }
    }

    private static class NodeStoppingPublisher<T> implements Publisher<T> {
        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            subscriber.onError(new NodeStoppingException());
        }
    }

    /**
     * Gets Meta storage service for test purpose.
     *
     * @return Meta storage service.
     */
    @TestOnly
    public MetaStorageServiceImpl getService() {
        return metaStorageSvcFut.join();
    }
}
