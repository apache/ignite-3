/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metastorage;

import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.network.util.ClusterServiceUtils.resolveNodes;

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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.client.CompactedException;
import org.apache.ignite.internal.metastorage.client.Condition;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.MetaStorageService;
import org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.internal.metastorage.client.OperationTimeoutException;
import org.apache.ignite.internal.metastorage.client.StatementResult;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.watch.AggregatedWatch;
import org.apache.ignite.internal.metastorage.watch.KeyCriterion;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
public class MetaStorageManager implements IgniteComponent {
    /** Meta storage raft group name. */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "metastorage_raft_group";

    /**
     * Special key for the vault where the applied revision for {@link MetaStorageManager#storeEntries} operation is stored. This mechanism
     * is needed for committing processed watches to {@link VaultManager}.
     */
    public static final ByteArray APPLIED_REV = ByteArray.fromString("applied_revision");

    private final ClusterService clusterService;

    /** Vault manager in order to commit processed watches with corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Raft manager that is used for metastorage raft group handling. */
    private final Loza raftMgr;

    private final ClusterManagementGroupManager cmgMgr;

    /** Meta storage service. */
    private volatile CompletableFuture<MetaStorageService> metaStorageSvcFut;

    /**
     * Aggregator of multiple watches to deploy them as one batch.
     *
     * @see WatchAggregator
     */
    private final WatchAggregator watchAggregator = new WatchAggregator();

    /**
     * Future which will be completed with {@link IgniteUuid}, when aggregated watch will be successfully deployed. Can be resolved to
     * {@code null} if no watch deployed at the moment.
     *
     * <p>Multi-threaded access is guarded by {@code this}.
     */
    private CompletableFuture<IgniteUuid> deployFut = new CompletableFuture<>();

    /** Actual storage for the Metastorage. */
    private final KeyValueStorage storage;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * If true - all new watches will be deployed immediately.
     *
     * <p>If false - all new watches will be aggregated to one batch for further deploy by {@link MetaStorageManager#deployWatches()}.
     *
     * <p>Multi-threaded access is guarded by {@code this}.
     */
    private boolean areWatchesDeployed = false;

    /**
     * Flag that indicates that the component has been initialized.
     */
    private volatile boolean isInitialized = false;

    /** Prevents double stopping the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param clusterService Cluster network service.
     * @param raftMgr Raft manager.
     * @param storage Storage. This component owns this resource and will manage its lifecycle.
     */
    public MetaStorageManager(
            VaultManager vaultMgr,
            ClusterService clusterService,
            ClusterManagementGroupManager cmgMgr,
            Loza raftMgr,
            KeyValueStorage storage
    ) {
        this.vaultMgr = vaultMgr;
        this.clusterService = clusterService;
        this.raftMgr = raftMgr;
        this.cmgMgr = cmgMgr;
        this.storage = storage;
    }

    private CompletableFuture<MetaStorageService> initializeMetaStorage(Collection<String> metaStorageNodes) {
        List<ClusterNode> metastorageNodes = resolveNodes(clusterService, metaStorageNodes);

        ClusterNode thisNode = clusterService.topologyService().localMember();

        if (metastorageNodes.contains(thisNode)) {
            clusterService.topologyService().addEventHandler(new TopologyEventHandler() {
                @Override
                public void onDisappeared(ClusterNode member) {
                    metaStorageSvcFut.thenAccept(svc -> svc.closeCursors(member.id()));
                }
            });
        }

        storage.start();

        try {
            CompletableFuture<RaftGroupService> raftServiceFuture = raftMgr.prepareRaftGroup(
                    METASTORAGE_RAFT_GROUP_NAME,
                    metastorageNodes,
                    () -> new MetaStorageListener(storage)
            );

            return raftServiceFuture.thenApply(service -> new MetaStorageServiceImpl(service, thisNode.id()));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        this.metaStorageSvcFut = cmgMgr.metaStorageNodes()
                // use default executor to avoid blocking CMG manager threads
                .thenComposeAsync(metaStorageNodes -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.failedFuture(new NodeStoppingException());
                    }

                    try {
                        isInitialized = true;

                        return initializeMetaStorage(metaStorageNodes);
                    } finally {
                        busyLock.leaveBusy();
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

        if (!isInitialized) {
            // Stop command was called before the init command was received
            return;
        }

        synchronized (this) {
            IgniteUtils.closeAll(
                    this::stopDeployedWatches,
                    () -> raftMgr.stopRaftGroup(METASTORAGE_RAFT_GROUP_NAME),
                    storage
            );
        }
    }

    private void stopDeployedWatches() throws Exception {
        if (!areWatchesDeployed) {
            return;
        }

        deployFut
                .thenCompose(watchId -> watchId == null
                        ? CompletableFuture.completedFuture(null)
                        : metaStorageSvcFut.thenAccept(service -> service.stopWatch(watchId))
                )
                .get();
    }

    /**
     * Deploy all registered watches.
     */
    public synchronized void deployWatches() throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            CompletableFuture<IgniteUuid> deployFut = this.deployFut;

            updateAggregatedWatch().whenComplete((id, ex) -> {
                if (ex == null) {
                    deployFut.complete(id);
                } else {
                    deployFut.completeExceptionally(ex);
                }
            });

            areWatchesDeployed = true;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Register watch listener by key.
     *
     * @param key The target key.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel subscription
     */
    public synchronized CompletableFuture<Long> registerWatch(ByteArray key, WatchListener lsnr) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            long watchId = watchAggregator.add(key, lsnr);

            return updateWatches().thenApply(uuid -> watchId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Register watch listener by collection of keys.
     *
     * @param keys Collection listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel subscription
     */
    public synchronized CompletableFuture<Long> registerWatch(Collection<ByteArray> keys, WatchListener lsnr) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            long watchId = watchAggregator.add(keys, lsnr);

            return updateWatches().thenApply(uuid -> watchId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Register watch listener by range of keys.
     *
     * @param from Start key of range.
     * @param to End key of range (exclusively).
     * @param lsnr Listener which will be notified for each update.
     * @return future with id of registered watch.
     */
    public synchronized CompletableFuture<Long> registerWatch(
            @NotNull ByteArray from,
            @NotNull ByteArray to,
            @NotNull WatchListener lsnr
    ) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            long watchId = watchAggregator.add(from, to, lsnr);

            return updateWatches().thenApply(uuid -> watchId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Register watch listener by key prefix.
     *
     * @param key Prefix to listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel subscription
     */
    public synchronized CompletableFuture<Long> registerWatchByPrefix(ByteArray key, WatchListener lsnr) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            long watchId = watchAggregator.addPrefix(key, lsnr);

            return updateWatches().thenApply(uuid -> watchId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Unregister watch listener by id.
     *
     * @param id of watch to unregister.
     * @return future, which will be completed when unregister finished.
     */
    public synchronized CompletableFuture<Void> unregisterWatch(long id) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            watchAggregator.cancel(id);

            return updateWatches().thenApply(uuid -> null);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves an entry for the given key.
     *
     * @see MetaStorageService#get(ByteArray)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves an entry for the given key and the revision upper bound.
     *
     * @see MetaStorageService#get(ByteArray, long)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key, revUpperBound));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for given keys.
     *
     * @see MetaStorageService#getAll(Set)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
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

    /**
     * Invoke with single success/failure operation.
     *
     * @see MetaStorageService#invoke(Condition, Operation, Operation)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition cond,
            @NotNull Operation success,
            @NotNull Operation failure
    ) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Updates an entry for the given key conditionally.
     *
     * @see MetaStorageService#invoke(Condition, Collection, Collection)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition cond,
            @NotNull Collection<Operation> success,
            @NotNull Collection<Operation> failure
    ) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Invoke, which supports nested conditional statements.
     *
     * @see MetaStorageService#invoke(If)
     */
    public @NotNull CompletableFuture<StatementResult> invoke(@NotNull If iif) {
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
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound)
            throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return new CursorWrapper<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, revUpperBound)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key range in lexicographic order.
     *
     * @see MetaStorageService#range(ByteArray, ByteArray)
     */
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return new CursorWrapper<>(metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo)));
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
            CompletableFuture<Cursor<Entry>> cursorFuture = metaStorageSvcFut.thenCombine(
                    appliedRevision(),
                    (svc, appliedRevision) -> svc.range(keyFrom, keyTo, appliedRevision)
            );

            return new CursorWrapper<>(cursorFuture);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Entries will be filtered out by the current applied revision as an
     * upper bound. Applied revision is a revision of the last successful vault update.
     *
     * <p>Prefix query is a synonym of the range query {@code (prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and applied revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefixWithAppliedRevision(@NotNull ByteArray keyPrefix) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            KeyCriterion.RangeCriterion rangeCriterion = KeyCriterion.RangeCriterion.fromPrefixKey(keyPrefix);

            CompletableFuture<Cursor<Entry>> cursorFuture = metaStorageSvcFut.thenCombine(
                    appliedRevision(),
                    (svc, appliedRevision) -> svc.range(rangeCriterion.from(), rangeCriterion.to(), appliedRevision)
            );

            return new CursorWrapper<>(cursorFuture);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Short cut for {@link #prefix(ByteArray, long)} where
     * {@code revUpperBound == -1}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefix(@NotNull ByteArray keyPrefix) throws NodeStoppingException {
        return prefix(keyPrefix, -1);
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * <p>Prefix query is a synonym of the range query {@code range(prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefix(@NotNull ByteArray keyPrefix, long revUpperBound) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            var rangeCriterion = KeyCriterion.RangeCriterion.fromPrefixKey(keyPrefix);
            return new CursorWrapper<>(
                    metaStorageSvcFut.thenApply(svc -> svc.range(rangeCriterion.from(), rangeCriterion.to(), revUpperBound))
            );
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
     * Returns applied revision for {@link VaultManager#putAll} operation.
     */
    private CompletableFuture<Long> appliedRevision() {
        return vaultMgr.get(APPLIED_REV)
                .thenApply(appliedRevision -> appliedRevision == null ? 0L : bytesToLong(appliedRevision.value()));
    }

    /**
     * Stop current batch of consolidated watches and register new one from current {@link WatchAggregator}.
     *
     * <p>This method MUST always be called under a {@code synchronized} block.
     *
     * @return Ignite UUID of new consolidated watch.
     */
    private CompletableFuture<IgniteUuid> updateWatches() {
        if (!areWatchesDeployed) {
            return deployFut;
        }

        deployFut = deployFut
                .thenCompose(id -> id == null
                        ? CompletableFuture.completedFuture(null)
                        : metaStorageSvcFut.thenCompose(svc -> svc.stopWatch(id))
                )
                .thenCompose(r -> updateAggregatedWatch());

        return deployFut;
    }

    private CompletableFuture<IgniteUuid> updateAggregatedWatch() {
        return appliedRevision()
                .thenCompose(appliedRevision ->
                        watchAggregator.watch(appliedRevision + 1, this::storeEntries)
                                .map(this::dispatchAppropriateMetaStorageWatch)
                                .orElseGet(() -> CompletableFuture.completedFuture(null))
                );
    }

    /**
     * Store entries with appropriate associated revision.
     *
     * @param entries to store.
     * @param revision associated revision.
     */
    private void storeEntries(Collection<IgniteBiTuple<ByteArray, byte[]>> entries, long revision) {
        appliedRevision()
                .thenCompose(appliedRevision -> {
                    if (revision <= appliedRevision) {
                        throw new IgniteInternalException(String.format(
                                "Current revision (%d) must be greater than the revision in the Vault (%d)",
                                revision, appliedRevision
                        ));
                    }

                    Map<ByteArray, byte[]> batch = IgniteUtils.newHashMap(entries.size() + 1);

                    batch.put(APPLIED_REV, longToBytes(revision));

                    entries.forEach(e -> batch.put(e.getKey(), e.getValue()));

                    return vaultMgr.putAll(batch);
                })
                .join();
    }

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.

    /** Cursor wrapper. */
    private final class CursorWrapper<T> implements Cursor<T> {
        /** Inner cursor future. */
        private final CompletableFuture<Cursor<T>> innerCursorFut;

        /** Inner iterator future. */
        private final CompletableFuture<Iterator<T>> innerIterFut;

        /**
         * Constructor.
         *
         * @param innerCursorFut Inner cursor future.
         */
        CursorWrapper(CompletableFuture<Cursor<T>> innerCursorFut) {
            this.innerCursorFut = innerCursorFut;
            this.innerIterFut = innerCursorFut.thenApply(Iterable::iterator);
        }

        /** {@inheritDoc} */
        @Override
        public void close() throws Exception {
            if (!busyLock.enterBusy()) {
                throw new NodeStoppingException();
            }

            try {
                innerCursorFut.thenAccept(cursor -> {
                    try {
                        cursor.close();
                    } catch (Exception e) {
                        throw new IgniteInternalException(e);
                    }
                }).get();
            } finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            if (!busyLock.enterBusy()) {
                return false;
            }

            try {
                try {
                    return innerIterFut.thenApply(Iterator::hasNext).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new IgniteInternalException(e);
                }
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
                try {
                    return innerIterFut.thenApply(Iterator::next).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new IgniteInternalException(e);
                }
            } finally {
                busyLock.leaveBusy();
            }
        }
    }

    /**
     * Dispatches appropriate metastorage watch method according to inferred watch criterion.
     *
     * @param aggregatedWatch Aggregated watch.
     * @return Future, which will be completed after new watch registration finished.
     */
    private CompletableFuture<IgniteUuid> dispatchAppropriateMetaStorageWatch(AggregatedWatch aggregatedWatch) {
        if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.CollectionCriterion) {
            var criterion = (KeyCriterion.CollectionCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                    criterion.keys(),
                    aggregatedWatch.revision(),
                    aggregatedWatch.listener()));
        } else if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.ExactCriterion) {
            var criterion = (KeyCriterion.ExactCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                    criterion.key(),
                    aggregatedWatch.revision(),
                    aggregatedWatch.listener()));
        } else if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.RangeCriterion) {
            var criterion = (KeyCriterion.RangeCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                    criterion.from(),
                    criterion.to(),
                    aggregatedWatch.revision(),
                    aggregatedWatch.listener()));
        } else {
            throw new UnsupportedOperationException("Unsupported type of criterion");
        }
    }
}
