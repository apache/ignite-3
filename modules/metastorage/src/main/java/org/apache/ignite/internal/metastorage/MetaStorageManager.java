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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.metastorage.MetaStorageConfiguration;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.client.CompactedException;
import org.apache.ignite.internal.metastorage.client.Condition;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.MetaStorageService;
import org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.internal.metastorage.client.OperationTimeoutException;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.message.MetastorageMessages;
import org.apache.ignite.internal.metastorage.message.MetastorageMessagesFactory;
import org.apache.ignite.internal.metastorage.message.MetastorageNodesRequest;
import org.apache.ignite.internal.metastorage.message.MetastorageNodesResponse;
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
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;

/**
 * MetaStorage manager is responsible for:
 * <ul>
 *     <li>Handling cluster init message.</li>
 *     <li>Managing meta storage lifecycle including instantiation meta storage raft group.</li>
 *     <li>Providing corresponding meta storage service proxy interface</li>
 * </ul>
 */
public class MetaStorageManager implements IgniteComponent {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(MetaStorageManager.class);

    /** Meta storage raft group name. */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "metastorage_raft_group";

    /**
     * Special key for the Vault, where the applied revision for {@link MetaStorageManager#storeEntries}
     * operation is stored. This mechanism is needed for committing processed watches to {@link VaultManager}.
     */
    public static final ByteArray APPLIED_REV = ByteArray.fromString("applied_revision");

    /** Vault manager, used to commit processed watches with the corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Cluster network service used to handle cluster init message. */
    private final ClusterService clusterNetSvc;

    /** Raft manager used for metastorage raft group handling. */
    private final Loza raftMgr;

    /**
     * Meta Storage service. Depends on the {@link #metaStorageNodeNamesFut} future and resolves only after it
     * completes.
     */
    private volatile CompletableFuture<MetaStorageService> metaStorageSvcFut;

    /** Raft group service. */
    private volatile RaftGroupService raftGroupService;

    /**
     * Future containing names of nodes that host the Meta Storage. This future completes after either receiving
     * the "init" command or obtaining the corresponding information from other nodes.
     */
    private final CompletableFuture<Collection<String>> metaStorageNodeNamesFut = new CompletableFuture<>();

    /** Messages factory. */
    private final MetastorageMessagesFactory messagesFactory;

    /**
     * Aggregator of multiple watches to deploy them as one batch.
     *
     * @see WatchAggregator
     */
    private final WatchAggregator watchAggregator = new WatchAggregator();

    /**
     * Future, which will be completed with a {@link IgniteUuid}, when aggregated watch is successfully deployed.
     * Can be resolved to {@code null} if no watch is deployed at the moment.
     */
    private CompletableFuture<IgniteUuid> deployFut = new CompletableFuture<>();

    /**
     * If true - all new watches will be deployed immediately.
     *
     * If false - all new watches will be aggregated to one batch
     * for further deploy by {@link MetaStorageManager#deployWatches()}
     */
    private boolean deployed;

    /** Local configuration. */
    private final MetaStorageConfiguration config;

    /** Actual storage for the Metastorage. */
    private final KeyValueStorage storage;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param clusterNetSvc Cluster network service.
     * @param raftMgr Raft manager.
     * @param messagesFactory Meta Storage messages factory.
     * @param config Configuration.
     * @param storage Storage. This component owns this resource and will manage its lifecycle.
     */
    public MetaStorageManager(
        VaultManager vaultMgr,
        ClusterService clusterNetSvc,
        Loza raftMgr,
        KeyValueStorage storage,
        MetastorageMessagesFactory messagesFactory,
        MetaStorageConfiguration config
    ) {
        this.vaultMgr = vaultMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.raftMgr = raftMgr;
        this.messagesFactory = messagesFactory;
        this.config = config;
        this.storage = storage;

        // register handler for responding to other nodes joining the cluster
        this.clusterNetSvc.messagingService().addMessageHandler(
            MetastorageMessages.class,
            (message, senderAddr, correlationId) -> {
                if (message instanceof MetastorageNodesRequest) {
                    NetworkMessage response;

                    if (metaStorageNodeNamesFut.isDone()) {
                        response = messagesFactory.metastorageNodesResponse()
                            .metastorageNodes(metaStorageNodeNamesFut.join())
                            .build();
                    }
                    else
                        response = messagesFactory.metastorageNotReadyResponse().build();

                    this.clusterNetSvc.messagingService().send(senderAddr, response, correlationId);
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override public void start() {
        assert metaStorageSvcFut == null : "Trying to start an already started MetaStorageManager";

        LOG.info("Starting the MetaStorage manager");

        storage.start();

        metaStorageSvcFut = joinCluster()
            .thenCompose(nodeNames -> raftMgr.prepareRaftGroup(
                METASTORAGE_RAFT_GROUP_NAME,
                metastorageNodes(nodeNames),
                () -> new MetaStorageListener(storage)
            ))
            .thenApply(raftGroup -> {
                raftGroupService = raftGroup;

                String localId = clusterNetSvc.topologyService().localMember().id();

                LOG.info("MetaStorage service has started");

                return new MetaStorageServiceImpl(raftGroup, localId);
            });

        metaStorageSvcFut.thenAccept(service -> {
            if (hasMetastorageLocally()) {
                clusterNetSvc.topologyService().addEventHandler(new TopologyEventHandler() {
                    @Override public void onAppeared(ClusterNode member) {
                        // No-op.
                    }

                    @Override public void onDisappeared(ClusterNode member) {
                        // TODO: there is a race between closing the cursor and stopping the whole node,
                        //  this listener must be removed on shutdown,
                        //  see https://issues.apache.org/jira/browse/IGNITE-14519
                        service
                            .closeCursors(member.id())
                            .whenComplete((v, ex) -> {
                                if (ex != null)
                                    LOG.error("Exception when closing cursors for " + member.id(), ex);
                            });
                    }
                });
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void beforeNodeStop() {
        LOG.info("Stopping the MetaStorage manager");

        // cancelling the metaStorageNodeNamesFut will also stop the polling process in metaStorageSvcFut
        metaStorageNodeNamesFut.cancel(false);

        try {
            metaStorageSvcFut.get(10, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {
            // execution exceptions are not important and are expected to be thrown, e.g. when the component stops
            // abruptly
            LOG.debug("Exception during MetaStorageManager stop", e.getCause());
        }
        catch (InterruptedException | TimeoutException e) {
            throw new IgniteInternalException("Exception during MetaStorageManager stop", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        busyLock.block();

        IgniteUuid watchId;

        try {
            // If deployed future is not done, that means that stop was called in the middle of
            // IgniteImpl.start, before deployWatches, or before init phase.
            // It is correct to check completeness of the future because the method calls are guarded by busy lock.
            // TODO: add busy lock for init method https://issues.apache.org/jira/browse/IGNITE-14414
            if (deployFut.isDone()) {
                watchId = deployFut.get();

                try {
                    if (watchId != null)
                        metaStorageSvcFut.get().stopWatch(watchId);
                }
                catch (InterruptedException | ExecutionException e) {
                    LOG.error("Failed to get meta storage service.");

                    throw new IgniteInternalException(e);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to get watch.");

            throw new IgniteInternalException(e);
        }

        // if the metaStorageNodeNamesFut is not done, it should be cancelled by the beforeNodeStop method.
        if (!metaStorageNodeNamesFut.isCancelled()) {
            Collection<String> nodeNames = metaStorageNodeNamesFut.join();

            raftGroupService.shutdown();

            raftMgr.stopRaftGroup(METASTORAGE_RAFT_GROUP_NAME, metastorageNodes(nodeNames));
        }

        try {
            storage.close();
        }
        catch (Exception e) {
            throw new IgniteInternalException("Exception when stopping the storage", e);
        }
    }

    /**
     * Temporary method for emulating the "init" command.
     *
     * @param metastorageNodeNames Names of the nodes hosting the Meta Storage.
     */
    // TODO: remove this method: https://issues.apache.org/jira/browse/IGNITE-14414
    public void init(List<String> metastorageNodeNames) {
        assert metaStorageSvcFut != null : "MetaStorageManager has not been started";

        metaStorageNodeNamesFut.complete(metastorageNodeNames);
    }

    /**
     * Tries to repeatedly retrieve information about the nodes hosting the Meta Storage from a random cluster member.
     */
    private CompletableFuture<Collection<String>> joinCluster() {
        if (metaStorageNodeNamesFut.isDone())
            return metaStorageNodeNamesFut;

        ClusterNode randomMember = randomMember();

        // if a random member is null, then this is the first node in the cluster and there's nothing to be done
        // until the "init" command arrives
        if (randomMember == null)
            return metaStorageNodeNamesFut;

        MetastorageNodesRequest request = messagesFactory.metastorageNodesRequest().build();

        LOG.info("Issuing a join request to {}", randomMember.address());

        return clusterNetSvc.messagingService()
            .invoke(randomMember, request, 1000)
            .handle((response, ex) -> {
                if (response instanceof MetastorageNodesResponse) {
                    var metastorageResponse = (MetastorageNodesResponse) response;

                    metaStorageNodeNamesFut.complete(metastorageResponse.metastorageNodes());

                    return metaStorageNodeNamesFut;
                }
                else {
                    if (ex != null)
                        LOG.debug("Exception while joining the cluster", ex);
                    else
                        LOG.debug("Received a {} response", response.getClass());

                    LOG.info(
                        "Received an error response, retrying in {} milliseconds",
                        config.startupPollIntervalMillis().value()
                    );

                    return supplyWithDelay(this::joinCluster);
                }
            })
            .thenCompose(Function.identity());
    }

    /**
     * Executes the given future (provided by the {@code supplier}) after a fixed delay.
     */
    private <T> CompletableFuture<T> supplyWithDelay(Supplier<CompletableFuture<T>> supplier) {
        long delay = config.startupPollIntervalMillis().value();

        Executor executor = CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS);

        return CompletableFuture.supplyAsync(supplier, executor)
            .thenCompose(Function.identity());
    }

    /**
     * Returns a random member from the topology or {@code null} if there are no other members, except the current node.
     */
    @Nullable
    private ClusterNode randomMember() {
        TopologyService topologyService = clusterNetSvc.topologyService();

        Collection<ClusterNode> members = topologyService.allMembers();

        if (members.size() <= 1)
            return null;

        // using size - 1 to filter out the local member
        int randomIndex = ThreadLocalRandom.current().nextInt(members.size() - 1);

        return members.stream()
            .filter(member -> !member.equals(topologyService.localMember()))
            .skip(randomIndex)
            .findFirst()
            .orElse(null);
    }

    /**
     * Deploy all registered watches.
     */
    public synchronized void deployWatches() throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

        try {
            Optional<AggregatedWatch> watch = watchAggregator.watch(appliedRevision() + 1, this::storeEntries);

            if (watch.isEmpty())
                deployFut.complete(null);
            else {
                // TODO: this method should be asynchronous, need to wait for this future in the init phase,
                //  see https://issues.apache.org/jira/browse/IGNITE-14414
                try {
                    metaStorageSvcFut
                        .thenCompose(service -> dispatchAppropriateMetaStorageWatch(watch.get()))
                        .thenAccept(deployFut::complete)
                        .get(1, TimeUnit.SECONDS);
                }
                catch (InterruptedException | ExecutionException | TimeoutException ignored) {
                }
            }

            deployed = true;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Register watch listener by key.
     *
     * @param key The target key.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatch(
        @Nullable ByteArray key,
        @NotNull WatchListener lsnr
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return waitForReDeploy(watchAggregator.add(key, lsnr));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Register watch listener by key prefix.
     *
     * @param key Prefix to listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatchByPrefix(
        @Nullable ByteArray key,
        @NotNull WatchListener lsnr
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return waitForReDeploy(watchAggregator.addPrefix(key, lsnr));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Register watch listener by collection of keys.
     *
     * @param keys Collection listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatch(
        @NotNull Collection<ByteArray> keys,
        @NotNull WatchListener lsnr
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return waitForReDeploy(watchAggregator.add(keys, lsnr));
        }
        finally {
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
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return waitForReDeploy(watchAggregator.add(from, to, lsnr));
        }
        finally {
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
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            watchAggregator.cancel(id);
            if (deployed)
                return updateWatches().thenAccept(v -> {});
            else
                return deployFut.thenAccept(uuid -> {});
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#get(ByteArray)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#get(ByteArray, long)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key, revUpperBound));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAll(Set)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAll(Set, long)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys, revUpperBound));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#put(ByteArray, byte[])
     */
    public @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, byte[] val) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.put(key, val));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAndPut(ByteArray, byte[])
     */
    public @NotNull CompletableFuture<Entry> getAndPut(@NotNull ByteArray key, byte[] val) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndPut(key, val));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#putAll(Map)
     */
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.putAll(vals));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAndPutAll(Map)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndPutAll(vals));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#remove(ByteArray)
     */
    public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.remove(key));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAndRemove(ByteArray)
     */
    public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemove(key));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#removeAll(Set)
     */
    public @NotNull CompletableFuture<Void> removeAll(@NotNull Set<ByteArray> keys) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.removeAll(keys));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAndRemoveAll(Set)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Set<ByteArray> keys) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemoveAll(keys));
        }
        finally {
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
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#invoke(Condition, Collection, Collection)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition cond,
            @NotNull Collection<Operation> success,
            @NotNull Collection<Operation> failure
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#range(ByteArray, ByteArray, long)
     */
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

        try {
            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, revUpperBound))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key range in lexicographic order.
     * Entries will be filtered out by the current applied revision as an upper bound.
     * Applied revision is a revision of the last successful vault update.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). Could be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and applied revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> rangeWithAppliedRevision(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

        try {
            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, appliedRevision()))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#range(ByteArray, ByteArray)
     */
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

        try {
            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order.
     * Entries will be filtered out by the current applied revision as an upper bound.
     * Applied revision is a revision of the last successful vault update.
     *
     * Prefix query is a synonym of the range query {@code (prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and applied revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefixWithAppliedRevision(@NotNull ByteArray keyPrefix) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

        try {
            var rangeCriterion = KeyCriterion.RangeCriterion.fromPrefixKey(keyPrefix);

            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(rangeCriterion.from(), rangeCriterion.to(), appliedRevision()))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Short cut for
     * {@link #prefix(ByteArray, long)} where {@code revUpperBound == -1}.
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
     * Retrieves entries for the given key prefix in lexicographic order. Entries will be filtered out by upper bound
     * of given revision number.
     *
     * Prefix query is a synonym of the range query {@code range(prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @param revUpperBound  The upper bound for entry revision. {@code -1} means latest revision.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefix(@NotNull ByteArray keyPrefix, long revUpperBound) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

        try {
            var rangeCriterion = KeyCriterion.RangeCriterion.fromPrefixKey(keyPrefix);
            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(rangeCriterion.from(), rangeCriterion.to(), revUpperBound))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#compact()
     */
    public @NotNull CompletableFuture<Void> compact() {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException("Operation has been cancelled (node is stopping)."));

        try {
            return metaStorageSvcFut.thenCompose(MetaStorageService::compact);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @return Applied revision for {@link VaultManager#putAll} operation.
     */
    private long appliedRevision() {
        byte[] appliedRevision = vaultMgr.get(APPLIED_REV).join().value();

        return appliedRevision == null ? 0L : bytesToLong(appliedRevision);
    }

    /**
     * Stop current batch of consolidated watches and register new one from current {@link WatchAggregator}.
     *
     * @return Ignite UUID of new consolidated watch.
     */
    private CompletableFuture<IgniteUuid> updateWatches() {
        long revision = appliedRevision() + 1;

        deployFut = deployFut
            .thenCompose(id ->
                id == null ? completedFuture(null) : metaStorageSvcFut.thenCompose(svc -> svc.stopWatch(id))
            )
            .thenCompose(r ->
                watchAggregator.watch(revision, this::storeEntries)
                    .map(this::dispatchAppropriateMetaStorageWatch)
                    .orElseGet(() -> completedFuture(null))
            );

        return deployFut;
    }

    /**
     * Store entries with appropriate associated revision.
     *
     * @param entries to store.
     * @param revision associated revision.
     */
    private void storeEntries(Collection<IgniteBiTuple<ByteArray, byte[]>> entries, long revision) {
        Map<ByteArray, byte[]> batch = IgniteUtils.newHashMap(entries.size() + 1);

        batch.put(APPLIED_REV, longToBytes(revision));

        entries.forEach(e -> batch.put(e.getKey(), e.getValue()));

        byte[] appliedRevisionBytes = vaultMgr.get(APPLIED_REV).join().value();

        long appliedRevision = appliedRevisionBytes == null ? 0L : bytesToLong(appliedRevisionBytes);

        if (revision <= appliedRevision) {
            throw new IgniteInternalException(String.format(
                "Current revision (%d) must be greater than the revision in the Vault (%d)",
                revision, appliedRevision
            ));
        }

        vaultMgr.putAll(batch).join();
    }

    /**
     * @param id of watch to redeploy.
     * @return future, which will be completed after redeploy finished.
     */
    private CompletableFuture<Long> waitForReDeploy(long id) {
        if (deployed)
            return updateWatches().thenApply(uid -> id);
        else
            return deployFut.thenApply(uid -> id);
    }

    /**
     * Checks whether the local node hosts meta storage.
     *
     * @return {@code true} if the node has meta storage, {@code false} otherwise.
     */
    public boolean hasMetastorageLocally() {
        return metaStorageNodeNamesFut
            .thenCompose(metastorageNodes ->
                vaultMgr.name().thenApply(name -> metastorageNodes.stream().anyMatch(name::equals))
            )
            .join();
    }

    // TODO: IGNITE-14691 Temporary solution that should be removed after implementing reactive watches.
    /** Cursor wrapper. */
    private final class CursorWrapper<T> implements Cursor<T> {
        /** Inner cursor future. */
        private final CompletableFuture<Cursor<T>> innerCursorFut;

        /**
         * @param innerCursorFut Inner cursor future.
         */
        CursorWrapper(CompletableFuture<Cursor<T>> innerCursorFut) {
            this.innerCursorFut = innerCursorFut;
        }

        /** {@inheritDoc} */
        @Override public void close() throws NodeStoppingException, InterruptedException, ExecutionException {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            try {
                cursor().close();
            }
            catch (Exception e) {
                throw new IgniteInternalException(e);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            if (!busyLock.enterBusy())
                return false;

            try {
                return cursor().hasNext();
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public T next() {
            if (!busyLock.enterBusy())
                throw new NoSuchElementException("No such element because node is stopping.");

            try {
                return cursor().next();
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<T> iterator() {
            return this;
        }

        /**
         * Waits for the wrapped future to complete.
         */
        private Cursor<T> cursor() {
            try {
                return innerCursorFut.get(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException | TimeoutException e) {
                throw new IgniteInternalException(e);
            }
            catch (ExecutionException e) {
                throw new IgniteInternalException(e.getCause());
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
        }
        else if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.ExactCriterion) {
            var criterion = (KeyCriterion.ExactCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                criterion.key(),
                aggregatedWatch.revision(),
                aggregatedWatch.listener()));
        }
        else if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.RangeCriterion) {
            var criterion = (KeyCriterion.RangeCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                criterion.from(),
                criterion.to(),
                aggregatedWatch.revision(),
                aggregatedWatch.listener()));
        }
        else
            throw new UnsupportedOperationException("Unsupported type of criterion");
    }

    /**
     * Return metastorage nodes.
     *
     * This code will be deleted after node init phase is developed.
     * https://issues.apache.org/jira/browse/IGNITE-14414
     */
    private List<ClusterNode> metastorageNodes(Collection<String> nodeNames) {
        return clusterNetSvc.topologyService()
            .allMembers()
            .stream()
            .filter(clusterNode -> nodeNames.contains(clusterNode.name()))
            .collect(Collectors.toUnmodifiableList());
    }
}
