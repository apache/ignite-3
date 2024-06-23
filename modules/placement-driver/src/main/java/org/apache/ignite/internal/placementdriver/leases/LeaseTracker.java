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

package org.apache.ignite.internal.placementdriver.leases;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED;
import static org.apache.ignite.internal.placementdriver.leases.Lease.emptyLease;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitException;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingIndependentComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class tracks cluster leases in memory.
 * At first, the class state recoveries from Vault, then updates on watch's listener.
 */
public class LeaseTracker extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters> implements PlacementDriver {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseTracker.class);

    /** Meta storage manager. */
    private final MetaStorageManager msManager;

    /** Busy lock to linearize service public API calls and service stop. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the tracker. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Leases cache. */
    private volatile Leases leases = new Leases(emptyMap(), BYTE_EMPTY_ARRAY);

    /** Map of primary replica waiters. */
    private final Map<ReplicationGroupId, PendingIndependentComparableValuesTracker<HybridTimestamp, ReplicaMeta>> primaryReplicaWaiters
            = new ConcurrentHashMap<>();

    /** Expiration future by replication group. */
    private final Map<ReplicationGroupId, CompletableFuture<Void>> expirationFutureByGroup = new ConcurrentHashMap<>();

    /** Listener to update a leases cache. */
    private final UpdateListener updateListener = new UpdateListener();

    /** Cluster node resolver. */
    private final ClusterNodeResolver clusterNodeResolver;

    private final ClockService clockService;

    /** Node name. */
    private final String nodeName;

    /** Repeated Meta storage lease subgroup updates will be handled in this thread pool. */
    private ExecutorService leaseUpdateRetryExecutor;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param msManager Meta storage manager.
     * @param clockService Clock service.
     */
    public LeaseTracker(
            String nodeName,
            MetaStorageManager msManager,
            ClusterNodeResolver clusterNodeResolver,
            ClockService clockService
    ) {
        this.nodeName = nodeName;
        this.msManager = msManager;
        this.clusterNodeResolver = clusterNodeResolver;
        this.clockService = clockService;
    }

    /**
     * Recovers state from Vault and subscribes to future updates.
     *
     * @param recoveryRevision Revision from {@link MetaStorageManager#recoveryFinishedFuture()}.
     */
    public void startTrack(long recoveryRevision) {
        inBusyLock(busyLock, () -> {
            msManager.registerExactWatch(PLACEMENTDRIVER_LEASES_KEY, updateListener);

            loadLeasesBusyAsync(recoveryRevision);

            leaseUpdateRetryExecutor = Executors.newSingleThreadExecutor(
                    NamedThreadFactory.create(nodeName, "lease-update-retry-executor", LOG)
            );
        });
    }

    @Override
    public CompletableFuture<Void> addSubgroups(
            ZonePartitionId zoneId,
            Long enlistmentConsistencyToken,
            Set<Integer> subGrps
    ) {
        if (leases.leaseByGroupId().get(zoneId).subgroups().containsAll(subGrps)) {
            return nullCompletedFuture();
        }

        CompletableFuture<Void> resultFut = new CompletableFuture<>();

        Leases leasesCurrent = leases;
        Map<ReplicationGroupId, Lease> previousLeasesMap = leasesCurrent.leaseByGroupId();
        Map<ReplicationGroupId, Lease> renewedLeases = new HashMap<>(previousLeasesMap);

        Lease previousLease = previousLeasesMap.get(zoneId);

        if (previousLease != null && enlistmentConsistencyToken.equals(previousLease.getStartTime().longValue())) {
            HashSet<Integer> subgroups = new HashSet<>(previousLease.subgroups());

            subgroups.addAll(subGrps);

            renewedLeases.put(
                    zoneId,
                    new Lease(
                        previousLease.getLeaseholder(),
                        previousLease.getLeaseholderId(),
                        previousLease.getStartTime(),
                        previousLease.getExpirationTime(),
                        previousLease.isProlongable(),
                        previousLease.isAccepted(),
                        null,
                        previousLease.replicationGroupId(),
                        subgroups
                    )
            );
        } else {
            resultFut.completeExceptionally(new PrimaryReplicaMissException(
                    nodeName,
                    previousLease == null ? null : previousLease.getLeaseholder(),
                    clusterNodeResolver.getByConsistentId(nodeName).id(),
                    previousLease == null ? null : previousLease.getLeaseholderId(),
                    enlistmentConsistencyToken,
                    previousLease == null ? null : previousLease.getStartTime().longValue(),
                    null
            ));

            return resultFut;
        }

        byte[] renewedValue = new LeaseBatch(renewedLeases.values()).bytes();

        msManager.invoke(
                or(notExists(PLACEMENTDRIVER_LEASES_KEY), value(PLACEMENTDRIVER_LEASES_KEY).eq(leasesCurrent.leasesBytes())),
                put(PLACEMENTDRIVER_LEASES_KEY, renewedValue),
                noop()
        ).whenCompleteAsync((invokeResult, throwable) -> {
            if (throwable != null) {
                resultFut.completeExceptionally(throwable);

                return;
            }

            if (invokeResult) {
                resultFut.complete(null);
            } else {
                try {
                    // Throttling.
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    LOG.error("The retry process is interrupted, so the subgroups won't be added [zoneId={}, subGrps={}].",
                            e,
                            zoneId,
                            subGrps
                    );

                    return;
                }

                addSubgroups(zoneId, enlistmentConsistencyToken, subGrps).whenComplete((unused, throwable1) -> {
                    if (throwable1 != null) {
                        resultFut.completeExceptionally(throwable1);
                    }

                    resultFut.complete(null);
                });
            }
        }, leaseUpdateRetryExecutor);

        return resultFut;
    }

    /** Stops the tracker. */
    public void stopTrack() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        primaryReplicaWaiters.forEach((groupId, pendingTracker) -> pendingTracker.close());
        primaryReplicaWaiters.clear();

        IgniteUtils.shutdownAndAwaitTermination(leaseUpdateRetryExecutor, 10, TimeUnit.SECONDS);

        msManager.unregisterWatch(updateListener);
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return expirationFutureByGroup.getOrDefault(grpId, nullCompletedFuture());
    }

    /**
     * Gets a lease for a particular group.
     *
     * @param grpId Replication group id.
     * @return A lease is associated with the group.
     */
    public Lease getLease(ReplicationGroupId grpId) {
        assert grpId instanceof ZonePartitionId : "Unexpected replication group type [grp=" + grpId + "].";

        assert ((ZonePartitionId) grpId).tableId() == 0;

        Leases leases = this.leases;

        assert leases != null : "Leases not initialized, probably the local placement driver actor hasn't started lease tracking.";

        Lease lease = leases.leaseByGroupId().get(grpId);

        return lease == null ? emptyLease(grpId) : lease;
    }

    @Override
    public ReplicaMeta getLeaseMeta(ReplicationGroupId grpId) {
        Lease lease = getLease(grpId);

        return lease.isAccepted() ? lease : null;
    }

    /** Returns collection of leases, ordered by replication group. */
    public Leases leasesCurrent() {
        return leases;
    }

    /** Listen lease holder updates. */
    private class UpdateListener implements WatchListener {
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            return inBusyLockAsync(busyLock, () -> {
                List<CompletableFuture<?>> fireEventFutures = new ArrayList<>();
                HashMap<Integer, Lease> expiredTablesLeases = new HashMap<>();

                for (EntryEvent entry : event.entryEvents()) {
                    Entry msEntry = entry.newEntry();

                    byte[] leasesBytes = msEntry.value();
                    Map<ReplicationGroupId, Lease> leasesMap = new HashMap<>();

                    LeaseBatch leaseBatch = LeaseBatch.fromBytes(ByteBuffer.wrap(leasesBytes).order(LITTLE_ENDIAN));

                    Map<ReplicationGroupId, Lease> previousLeasesMap = leases.leaseByGroupId();

                    for (Lease lease : leaseBatch.leases()) {
                        ReplicationGroupId grpId = lease.replicationGroupId();

                        leasesMap.put(grpId, lease);

                        Lease previousLease = previousLeasesMap.get(grpId);

                        if (lease.isAccepted()) {
                            primaryReplicaWaiters
                                    .computeIfAbsent(grpId, groupId -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE))
                                    .update(lease.getExpirationTime(), lease);

                            for (Integer tableToNotify : needFireEventReplicaBecomePrimary(previousLease, lease)) {
                                fireEventFutures.add(fireEventReplicaBecomePrimary(tableToNotify, event.revision(), lease));
                            }
                        }

                        if (previousLease != null && previousLease.isAccepted()) {
                            for (Integer tableToNotify : needFireEventReplicaExpired(previousLease, lease)) {
                                expiredTablesLeases.put(tableToNotify, previousLease);
                            }
                        }
                    }

                    for (Map.Entry<ReplicationGroupId, Lease> replicaLease : previousLeasesMap.entrySet()) {
                        ReplicationGroupId grpId = replicaLease.getKey();

                        if (!leasesMap.containsKey(grpId)) {
                            tryRemoveTracker(grpId);

                            Lease previousLease = previousLeasesMap.get(grpId);

                            if (previousLease.isAccepted()) {
                                for (Integer tableToNotify : needFireEventReplicaExpired(previousLease, null)) {
                                    expiredTablesLeases.put(tableToNotify, previousLease);
                                }
                            }
                        }
                    }

                    leases = new Leases(unmodifiableMap(leasesMap), leasesBytes);

                    for (Map.Entry<Integer, Lease> expiredTableLease : expiredTablesLeases.entrySet()) {
                        fireEventPrimaryReplicaExpired(expiredTableLease.getKey(), event.revision(), expiredTableLease.getValue());
                    }
                }

                return allOf(fireEventFutures.toArray(CompletableFuture[]::new));
            });
        }

        @Override
        public void onError(Throwable e) {
            LOG.warn("Unable to process update leases event", e);
        }
    }

    private void awaitPrimaryReplica(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            CompletableFuture<ReplicaMeta> resultFuture
    ) {
        inBusyLockAsync(busyLock, () -> getOrCreatePrimaryReplicaWaiter(groupId).waitFor(timestamp)
                .thenAccept(replicaMeta -> {
                    ClusterNode leaseholderNode = clusterNodeResolver.getById(replicaMeta.getLeaseholderId());

                    if (leaseholderNode == null && !resultFuture.isDone()) {
                        awaitPrimaryReplica(
                                groupId,
                                replicaMeta.getExpirationTime().tick(),
                                resultFuture
                        );
                    } else {
                        resultFuture.complete(replicaMeta);
                    }
                })
        );
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        assert groupId instanceof ZonePartitionId : "Unexpected replication group type [grp=" + groupId + "].";

        return awaitPrimaryReplicaForTable(groupId, timestamp, timeout, unit);
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplicaForTable(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        assert groupId instanceof ZonePartitionId : "Unexpected replication group type [grp=" + groupId + "].";

        var zonePartId = ZonePartitionId.resetTableId(((ZonePartitionId) groupId));

        CompletableFuture<ReplicaMeta> future = new CompletableFuture<>();

        awaitPrimaryReplica(zonePartId, timestamp, future);

        return future
                .orTimeout(timeout, unit)
                .exceptionally(e -> {
                    if (e instanceof TimeoutException) {
                        throw new PrimaryReplicaAwaitTimeoutException(zonePartId, timestamp, leases.leaseByGroupId().get(zonePartId), e);
                    }

                    throw new PrimaryReplicaAwaitException(zonePartId, timestamp, e);
                });
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp) {
        assert groupId instanceof ZonePartitionId : "Unexpected replication group type [grp=" + groupId + "].";

        return getPrimaryReplicaForTable(groupId, timestamp);
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplicaForTable(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        assert replicationGroupId instanceof ZonePartitionId : "Unexpected replication group type [grp=" + replicationGroupId + "].";

        var zonePartId = ZonePartitionId.resetTableId(((ZonePartitionId) replicationGroupId));

        return inBusyLockAsync(busyLock, () -> {
            Lease lease = getLease(zonePartId);

            if (lease.isAccepted() && clockService.after(lease.getExpirationTime(), timestamp)) {
                return completedFuture(lease);
            }

            return msManager
                    .clusterTime()
                    .waitFor(timestamp.addPhysicalTime(clockService.maxClockSkewMillis()))
                    .thenApply(ignored -> inBusyLock(busyLock, () -> {
                        Lease lease0 = getLease(zonePartId);

                        if (lease0.isAccepted() && clockService.after(lease0.getExpirationTime(), timestamp)) {
                            return lease0;
                        } else {
                            return null;
                        }
                    }));
        });
    }

    /**
     * Helper method that checks whether tracker for given groupId is present in {@code primaryReplicaWaiters} map, whether it's empty
     * and removes it if it's true.
     *
     * @param groupId Replication group id.
     */
    private void tryRemoveTracker(ReplicationGroupId groupId) {
        primaryReplicaWaiters.compute(groupId, (groupId0, tracker0) -> {
            if (tracker0 != null && tracker0.isEmpty()) {
                return null;
            }

            return tracker0;
        });
    }

    private PendingIndependentComparableValuesTracker<HybridTimestamp, ReplicaMeta> getOrCreatePrimaryReplicaWaiter(
            ReplicationGroupId groupId
    ) {
        return primaryReplicaWaiters.computeIfAbsent(groupId, key -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE));
    }

    private void loadLeasesBusyAsync(long recoveryRevision) {
        Entry entry = msManager.getLocally(PLACEMENTDRIVER_LEASES_KEY, recoveryRevision);

        if (entry.empty() || entry.tombstone()) {
            leases = new Leases(Map.of(), BYTE_EMPTY_ARRAY);
        } else {
            byte[] leasesBytes = entry.value();

            LeaseBatch leaseBatch = LeaseBatch.fromBytes(ByteBuffer.wrap(leasesBytes).order(LITTLE_ENDIAN));

            Map<ReplicationGroupId, Lease> leasesMap = new HashMap<>();

            leaseBatch.leases().forEach(lease -> {
                ReplicationGroupId grpId = lease.replicationGroupId();

                leasesMap.put(grpId, lease);

                if (lease.isAccepted()) {
                    getOrCreatePrimaryReplicaWaiter(grpId).update(lease.getExpirationTime(), lease);
                }
            });

            leases = new Leases(unmodifiableMap(leasesMap), leasesBytes);
        }

        LOG.info("Leases cache recovered [leases={}]", leases);
    }

    /**
     * Fires the primary replica expire event if it needs.
     *
     * @param previousLease Lease to check on expiration.
     * @param newLease A new lease.
     * @return Collection of replication group ids, which are needed to be notified.
     */
    private Set<Integer> needFireEventReplicaExpired(Lease previousLease, @Nullable Lease newLease) {
        assert previousLease.isAccepted() : previousLease;

        if (newLease == null || !newLease.isAccepted() || !newLease.getStartTime().equals(previousLease.getStartTime())) {
            return previousLease.subgroups();
        }

        Set<Integer> needToBeNotified = new HashSet<>(previousLease.subgroups());

        needToBeNotified.removeAll(newLease.subgroups());

        return needToBeNotified;
    }

    /**
     * Fires the primary replica expire event.
     *
     * @param tableId Table id.
     * @param causalityToken Causality token.
     * @param expiredLease Expired lease.
     */
    private void fireEventPrimaryReplicaExpired(Integer tableId, long causalityToken, Lease expiredLease) {
        ZonePartitionId groupId = (ZonePartitionId) expiredLease.replicationGroupId();

        CompletableFuture<Void> fut = fireEvent(
                PRIMARY_REPLICA_EXPIRED,
                new PrimaryReplicaEventParameters(
                        causalityToken,
                        new ZonePartitionId(groupId.zoneId(), tableId, groupId.partitionId()),
                        expiredLease.getLeaseholderId(),
                        expiredLease.getLeaseholder(),
                        expiredLease.getStartTime()
                )
        );

        CompletableFuture<Void> prev = expirationFutureByGroup.put(
                groupId,
                fut
        );

        assert prev == null || prev.isDone() :
                "Previous lease expiration process has not completed yet [grpId=" + expiredLease.replicationGroupId()
                        + ", subGrpId=" + groupId + ']';
    }

    /**
     * Fires the replica become primary event.
     *
     * @param tableId Table id.
     * @param causalityToken Causality token.
     * @param lease A new lease.
     * @return Future to notification complete.
     */
    private CompletableFuture<Void> fireEventReplicaBecomePrimary(Integer tableId, long causalityToken, Lease lease) {
        String leaseholderId = lease.getLeaseholderId();

        ZonePartitionId groupId = (ZonePartitionId) lease.replicationGroupId();

        assert leaseholderId != null : lease;

        return fireEvent(
                PRIMARY_REPLICA_ELECTED,
                new PrimaryReplicaEventParameters(
                        causalityToken,
                        new ZonePartitionId(groupId.zoneId(), tableId, groupId.partitionId()),
                        leaseholderId,
                        lease.getLeaseholder(),
                        lease.getStartTime()
                )
        );
    }

    /**
     * Checks whether event {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} should be fired for an <b>accepted</b> lease.
     *
     * @param previousLease Previous group lease, {@code null} if absent.
     * @param newLease New group lease.
     * @return Collection of replication group ids, which are needed to be notified.
     */
    private static Set<Integer> needFireEventReplicaBecomePrimary(@Nullable Lease previousLease, Lease newLease) {
        assert newLease.isAccepted() : newLease;

        Set<Integer> tableIds = newLease.subgroups();

        if (previousLease == null || !previousLease.isAccepted() || !previousLease.getStartTime().equals(newLease.getStartTime())) {
            return tableIds;
        }

        Set<Integer> needToBeNotified = new HashSet<>(tableIds);

        needToBeNotified.removeAll(previousLease.subgroups());

        return needToBeNotified;
    }
}
