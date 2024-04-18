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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitException;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingIndependentComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeResolver;
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

    private final Function<TablePartitionId, ZonePartitionId> tablePartIdToZoneIdProvider;

    private final ClockService clockService;

    /**
     * Constructor.
     *
     * @param msManager Meta storage manager.
     * @param clockService Clock service.
     */
    public LeaseTracker(
            MetaStorageManager msManager,
            ClusterNodeResolver clusterNodeResolver,
            ClockService clockService,
            Function<TablePartitionId, ZonePartitionId> tablePartIdToZoneIdProvider
    ) {
        this.msManager = msManager;
        this.clusterNodeResolver = clusterNodeResolver;
        this.clockService = clockService;
        this.tablePartIdToZoneIdProvider = tablePartIdToZoneIdProvider;
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
        });
    }

    @Override
    public CompletableFuture<Void> addSubgroups(
            ZonePartitionId zoneId,
            Long enlistmentConsistencyToken,
            Set<ReplicationGroupId> subGrps
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
            HashSet<ReplicationGroupId> subgroups = new HashSet<>(previousLease.subgroups());

            subgroups.addAll(subGrps);

            renewedLeases.put(zoneId, new Lease(
                    previousLease.getLeaseholder(),
                    previousLease.getLeaseholderId(),
                    previousLease.getStartTime(),
                    previousLease.getExpirationTime(),
                    previousLease.isProlongable(),
                    previousLease.isAccepted(),
                    null,
                    previousLease.replicationGroupId(),
                    subgroups));
        } else {
            resultFut.completeExceptionally(new PrimaryReplicaMissException(
                    "localNode.name()",
                    null,
                    "localNode.id()",
                    null,
                    null,
                    null,
                    null
            ));
        }

        byte[] renewedValue = new LeaseBatch(renewedLeases.values()).bytes();

        msManager.invoke(
                or(notExists(PLACEMENTDRIVER_LEASES_KEY), value(PLACEMENTDRIVER_LEASES_KEY).eq(leasesCurrent.leasesBytes())),
                put(PLACEMENTDRIVER_LEASES_KEY, renewedValue),
                noop()
        ).whenComplete((invokeResult, throwable) -> {
            if (throwable != null) {
                resultFut.completeExceptionally(throwable);

                return;
            }

            if (invokeResult) {
                resultFut.complete(null);
            } else {
                addSubgroups(zoneId, enlistmentConsistencyToken, subGrps).whenComplete((unused, throwable1) -> {
                    if (throwable1 != null) {
                        resultFut.completeExceptionally(throwable1);
                    }

                    resultFut.complete(null);
                });
            }
        });

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

        msManager.unregisterWatch(updateListener);
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return expirationFutureByGroup.getOrDefault(grpId, nullCompletedFuture());
    }

    @Override
    public ReplicaMeta currentLease(ReplicationGroupId groupId) {
        assert groupId instanceof TablePartitionId : "Unexpected replication group type [grp=" + groupId + "].";

        var tblPartId = (TablePartitionId) groupId;

        ReplicationGroupId groupId0 = tablePartIdToZoneIdProvider.apply(tblPartId);

        return inBusyLock(busyLock, () -> {
            Lease lease = getLease(groupId0);

            return lease.isAccepted() ? lease : null;
        });
    }

    /**
     * Gets a lease for a particular group.
     *
     * @param grpId Replication group id.
     * @return A lease is associated with the group.
     */
    public Lease getLease(ReplicationGroupId grpId) {
        assert grpId instanceof ZonePartitionId : "Unexpected replication group type [grp=" + grpId + "].";

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
                HashMap<ReplicationGroupId, Lease> expiredLeases = new HashMap<>();

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

                            for (ReplicationGroupId groupToNotify : needFireEventReplicaBecomePrimary(previousLease, lease)) {
                                fireEventFutures.add(fireEventReplicaBecomePrimary(groupToNotify, event.revision(), lease));
                            }
                        }

                        if (previousLease != null && previousLease.isAccepted()) {
                            for (ReplicationGroupId groupToNotify : needFireEventReplicaExpired(previousLease, lease)) {
                                expiredLeases.put(groupToNotify, previousLease);
                            }
                        }
                    }

                    for (Map.Entry<ReplicationGroupId, Lease> replicaLease : previousLeasesMap.entrySet()) {
                        ReplicationGroupId grpId = replicaLease.getKey();

                        if (!leasesMap.containsKey(grpId)) {
                            tryRemoveTracker(grpId);

                            Lease previousLease = previousLeasesMap.get(grpId);

                            if (previousLease.isAccepted()) {
                                for (ReplicationGroupId groupToNotify : needFireEventReplicaExpired(previousLease, null)) {
                                    expiredLeases.put(groupToNotify, previousLease);
                                }
                            }
                        }
                    }

                    leases = new Leases(unmodifiableMap(leasesMap), leasesBytes);

                    for (Map.Entry<ReplicationGroupId, Lease> expiredLease : expiredLeases.entrySet()) {
                        fireEventPrimaryReplicaExpired(expiredLease.getKey(), event.revision(), expiredLease.getValue());
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
        assert groupId instanceof TablePartitionId : "Unexpected replication group type [grp=" + groupId + "].";

        var tblPartId = (TablePartitionId) groupId;

        ReplicationGroupId groupId0 = tablePartIdToZoneIdProvider.apply(tblPartId);

        CompletableFuture<ReplicaMeta> future = new CompletableFuture<>();

        awaitPrimaryReplica(groupId0, timestamp, future);

        return future
                .orTimeout(timeout, unit)
                .exceptionally(e -> {
                    if (e instanceof TimeoutException) {
                        throw new PrimaryReplicaAwaitTimeoutException(groupId0, timestamp, leases.leaseByGroupId().get(groupId0), e);
                    }

                    throw new PrimaryReplicaAwaitException(groupId0, timestamp, e);
                });
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp) {
        assert groupId instanceof TablePartitionId : "Unexpected replication group type [grp=" + groupId + "].";

        var tblPartId = (TablePartitionId) groupId;

        ReplicationGroupId groupId0 = tablePartIdToZoneIdProvider.apply(tblPartId);

        return inBusyLockAsync(busyLock, () -> {
            Lease lease = getLease(groupId0);

            if (lease.isAccepted() && clockService.after(lease.getExpirationTime(), timestamp)) {
                return completedFuture(lease);
            }

            return msManager
                    .clusterTime()
                    .waitFor(timestamp.addPhysicalTime(clockService.maxClockSkewMillis()))
                    .thenApply(ignored -> inBusyLock(busyLock, () -> {
                        Lease lease0 = getLease(groupId0);

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
    private Set<ReplicationGroupId> needFireEventReplicaExpired(Lease previousLease, @Nullable Lease newLease) {
        assert previousLease.isAccepted() : previousLease;

        if (newLease == null || !newLease.isAccepted() || !newLease.getStartTime().equals(previousLease.getStartTime())) {
            return previousLease.subgroups();
        }

        Set<ReplicationGroupId> needToBeNotified = new HashSet<>(previousLease.subgroups());

        needToBeNotified.removeAll(newLease.subgroups());

        return needToBeNotified;
    }

    /**
     * Fires the primary replica expire event.
     *
     * @param groupId Replication group id.
     * @param causalityToken Causality token.
     * @param expiredLease Expired lease.
     */
    private void fireEventPrimaryReplicaExpired(ReplicationGroupId groupId, long causalityToken, Lease expiredLease) {
        TablePartitionId tablePartitionId = (TablePartitionId) groupId;

        ZonePartitionId zonePartitionId = (ZonePartitionId) expiredLease.replicationGroupId();

        CompletableFuture<Void> fut = fireEvent(
                PRIMARY_REPLICA_EXPIRED,
                new PrimaryReplicaEventParameters(
                        causalityToken,
                        new ZonePartitionId(zonePartitionId.zoneId(), tablePartitionId.tableId(), zonePartitionId.partitionId()),
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
     * @param groupId Replication group id.
     * @param causalityToken Causality token.
     * @param lease A new lease.
     * @return Future to notification complete.
     */
    private CompletableFuture<Void> fireEventReplicaBecomePrimary(ReplicationGroupId groupId, long causalityToken, Lease lease) {
        String leaseholderId = lease.getLeaseholderId();

        ZonePartitionId zonePartitionId = (ZonePartitionId) lease.replicationGroupId();

        TablePartitionId tablePartitionId = (TablePartitionId) groupId;

        assert leaseholderId != null : lease;

        return fireEvent(
                PRIMARY_REPLICA_ELECTED,
                new PrimaryReplicaEventParameters(
                        causalityToken,
                        new ZonePartitionId(zonePartitionId.zoneId(), tablePartitionId.tableId(), zonePartitionId.partitionId()),
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
    private static Set<ReplicationGroupId> needFireEventReplicaBecomePrimary(@Nullable Lease previousLease, Lease newLease) {
        assert newLease.isAccepted() : newLease;

        if (previousLease == null || !previousLease.isAccepted() || !previousLease.getStartTime().equals(newLease.getStartTime())) {
            return newLease.subgroups();
        }

        Set<ReplicationGroupId> needToBeNotified = new HashSet<>(newLease.subgroups());

        needToBeNotified.removeAll(previousLease.subgroups());

        return needToBeNotified;
    }
}
