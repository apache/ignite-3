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

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.Utils.extractZoneIdFromGroupId;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED;
import static org.apache.ignite.internal.placementdriver.leases.Lease.emptyLease;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.distributionzones.exception.EmptyDataNodesException;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitException;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.PendingIndependentComparableValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.jetbrains.annotations.Nullable;

/**
 * Class that tracks cluster leases in memory.
 */
public class LeaseTracker extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters> implements
        LeasePlacementDriver {
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

    private final Function<Integer, CompletableFuture<Set<String>>> currentDataNodesProvider;

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
            Function<Integer, CompletableFuture<Set<String>>> currentDataNodesProvider
    ) {
        this.msManager = msManager;
        this.clusterNodeResolver = clusterNodeResolver;
        this.clockService = clockService;
        this.currentDataNodesProvider = currentDataNodesProvider;
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

    /** Stops the tracker. */
    public void stopTrack() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        primaryReplicaWaiters.values().forEach(PendingComparableValuesTracker::close);
        primaryReplicaWaiters.clear();

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
        Leases leases = this.leases;

        assert leases != null : "Leases not initialized, probably the local placement driver actor hasn't started lease tracking.";

        Lease lease = leases.leaseByGroupId().get(grpId);

        return lease == null ? emptyLease(grpId) : lease;
    }

    /** Returns collection of latest leases, ordered by replication group. Shows all latest leases including expired ones. */
    public Leases leasesLatest() {
        return leases;
    }

    /** Listen lease holder updates. */
    private class UpdateListener implements WatchListener {
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            return inBusyLockAsync(busyLock, () -> {
                var eventsToFire = new ArrayList<Supplier<CompletableFuture<?>>>();

                long eventRevision = event.revision();

                byte[] leasesBytes = event.entryEvent().newEntry().value();

                assert leasesBytes != null;

                LeaseBatch leaseBatch = LeaseBatch.fromBytes(leasesBytes);

                Map<ReplicationGroupId, Lease> newLeasesMap = newHashMap(leaseBatch.leases().size());

                Map<ReplicationGroupId, Lease> previousLeasesMap = leases.leaseByGroupId();

                for (Lease newLease : leaseBatch.leases()) {
                    ReplicationGroupId grpId = newLease.replicationGroupId();

                    newLeasesMap.put(grpId, newLease);

                    if (newLease.isAccepted()) {
                        getOrCreatePrimaryReplicaWaiter(grpId).update(newLease.getExpirationTime(), newLease);
                    }

                    Lease previousLease = previousLeasesMap.get(grpId);

                    enqueuePrimaryReplicaEvents(eventsToFire, previousLease, newLease, eventRevision);
                }

                // Check leases that were not present in the new update.
                for (Map.Entry<ReplicationGroupId, Lease> e : previousLeasesMap.entrySet()) {
                    ReplicationGroupId grpId = e.getKey();

                    if (!newLeasesMap.containsKey(grpId)) {
                        tryRemoveTracker(grpId);

                        Lease previousLease = e.getValue();

                        enqueuePrimaryReplicaEvents(eventsToFire, previousLease, null, eventRevision);
                    }
                }

                leases = new Leases(newLeasesMap, leasesBytes);

                var eventFutures = new CompletableFuture<?>[eventsToFire.size()];

                for (int i = 0; i < eventsToFire.size(); i++) {
                    eventFutures[i] = eventsToFire.get(i).get();
                }

                return allOf(eventFutures);
            });
        }
    }

    private void enqueuePrimaryReplicaEvents(
            List<Supplier<CompletableFuture<?>>> eventsQueue,
            @Nullable Lease previousLease,
            @Nullable Lease newLease,
            long causalityToken
    ) {
        boolean needToFirePrimaryExpiredEvent = needToFirePrimaryReplicaExpiredEvent(previousLease, newLease);

        boolean needToFirePrimaryElectedEvent = needToFirePrimaryReplicaElectedEvent(previousLease, newLease);

        // If we need to fire both events simultaneously, we have to linearize them by firing the election event strictly after
        // the expiration event has been handled.
        if (needToFirePrimaryElectedEvent && needToFirePrimaryExpiredEvent) {
            assert previousLease != null;
            assert newLease != null;

            eventsQueue.add(() -> firePrimaryReplicaExpiredEvent(causalityToken, previousLease)
                    .thenCompose(v -> firePrimaryReplicaElectedEvent(causalityToken, newLease)));
        } else if (needToFirePrimaryExpiredEvent) {
            assert previousLease != null;

            eventsQueue.add(() -> firePrimaryReplicaExpiredEvent(causalityToken, previousLease));
        } else if (needToFirePrimaryElectedEvent) {
            assert newLease != null;

            eventsQueue.add(() -> firePrimaryReplicaElectedEvent(causalityToken, newLease));
        }
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        return inBusyLockAsync(busyLock, () -> {
            ReplicaMeta currentMeta = getCurrentPrimaryReplica(groupId, timestamp);

            if (isValidReplicaMeta(currentMeta)) {
                return completedFuture(currentMeta);
            }

            return awaitPrimaryReplicaImpl(groupId, timestamp, timeout, unit);
        });
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplicaImpl(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        return awaitPrimaryReplicaImpl(groupId, timestamp, System.nanoTime(), unit.toNanos(timeout))
                .handle((replicaMeta, e) -> {
                    if (e == null) {
                        return completedFuture(replicaMeta);
                    } else {
                        CompletableFuture<ReplicaMeta> failed = new CompletableFuture<>();

                        if (hasCause(e, TimeoutException.class)) {
                            checkDataNodes(groupId)
                                    .thenRun(() -> {
                                        throw new PrimaryReplicaAwaitTimeoutException(
                                                groupId,
                                                timestamp,
                                                leases.leaseByGroupId().get(groupId),
                                                e
                                        );
                                    })
                                    .exceptionally(ex -> {
                                        failed.completeExceptionally(ex);
                                        return null;
                                    });
                        } else if (hasCause(e, TrackerClosedException.class)) {
                            // TrackerClosedException is thrown when trackers are closed on node stop.
                            failed.completeExceptionally(new CompletionException(new NodeStoppingException(e)));
                        } else {
                            failed.completeExceptionally(new PrimaryReplicaAwaitException(groupId, timestamp, e));
                        }

                        return failed;
                    }
                })
                .thenCompose(identity());
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplicaImpl(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long startNanoTime,
            long timeoutNanos
    ) {
        return inBusyLockAsync(busyLock, () -> {
            long elapsedNanos = System.nanoTime() - startNanoTime;

            long remainingTimeoutNanos = timeoutNanos - elapsedNanos;

            if (remainingTimeoutNanos <= 0) {
                return failedFuture(new TimeoutException());
            }

            return getOrCreatePrimaryReplicaWaiter(groupId)
                    .waitFor(timestamp)
                    .orTimeout(remainingTimeoutNanos, TimeUnit.NANOSECONDS)
                    .thenCompose(replicaMeta -> {
                        if (isValidReplicaMeta(replicaMeta)) {
                            return completedFuture(replicaMeta);
                        }

                        return awaitPrimaryReplicaImpl(groupId, replicaMeta.getExpirationTime().tick(), startNanoTime, timeoutNanos);
                    });
        });
    }

    private CompletableFuture<Void> checkDataNodes(ReplicationGroupId groupId) {
        int zoneId = extractZoneIdFromGroupId(groupId);

        return currentDataNodesProvider.apply(zoneId)
                .thenAccept(dataNodes -> {
                    if (dataNodes.isEmpty()) {
                        throw new EmptyDataNodesException(zoneId);
                    }
                });
    }

    private boolean isValidReplicaMeta(@Nullable ReplicaMeta replicaMeta) {
        UUID leaseholderId = replicaMeta == null ? null : replicaMeta.getLeaseholderId();

        return leaseholderId != null && clusterNodeResolver.getById(leaseholderId) != null;
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return inBusyLockAsync(busyLock, () -> {
            Lease lease = getLease(replicationGroupId);

            if (lease.isAccepted() && clockService.after(lease.getExpirationTime(), timestamp)) {
                return completedFuture(lease);
            }

            return msManager
                    .clusterTime()
                    .waitFor(timestamp.addPhysicalTime(clockService.maxClockSkewMillis()))
                    .thenApply(ignored -> inBusyLock(busyLock, () -> {
                        Lease lease0 = getLease(replicationGroupId);

                        if (lease0.isAccepted() && clockService.after(lease0.getExpirationTime(), timestamp)) {
                            return lease0;
                        } else {
                            return null;
                        }
                    }));
        });
    }

    @Override
    public @Nullable ReplicaMeta getCurrentPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        Lease lease = getLease(replicationGroupId);

        if (lease.isAccepted() && clockService.after(lease.getExpirationTime(), timestamp)) {
            return lease;
        }

        return null;
    }

    /**
     * Helper method that checks whether tracker for given groupId is present in {@code primaryReplicaWaiters} map, whether it's empty and
     * removes it if it's true.
     *
     * @param groupId Replication group id.
     */
    private void tryRemoveTracker(ReplicationGroupId groupId) {
        primaryReplicaWaiters.computeIfPresent(groupId, (groupId0, tracker0) -> {
            if (tracker0.isEmpty()) {
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

            assert leasesBytes != null;

            LeaseBatch leaseBatch = LeaseBatch.fromBytes(leasesBytes);

            Map<ReplicationGroupId, Lease> leasesMap = newHashMap(leaseBatch.leases().size());

            leaseBatch.leases().forEach(lease -> {
                ReplicationGroupId grpId = lease.replicationGroupId();

                leasesMap.put(grpId, lease);

                if (lease.isAccepted()) {
                    getOrCreatePrimaryReplicaWaiter(grpId).update(lease.getExpirationTime(), lease);
                }
            });

            leases = new Leases(leasesMap, leasesBytes);
        }

        LOG.info("Leases cache recovered [leases={}]", leases);
    }

    /**
     * Fires the primary replica expire event.
     *
     * @param causalityToken Causality token.
     * @param expiredLease Expired lease.
     */
    private CompletableFuture<Void> firePrimaryReplicaExpiredEvent(long causalityToken, Lease expiredLease) {
        ReplicationGroupId grpId = expiredLease.replicationGroupId();

        CompletableFuture<Void> eventFuture = fireEvent(
                PRIMARY_REPLICA_EXPIRED,
                new PrimaryReplicaEventParameters(
                        causalityToken,
                        grpId,
                        expiredLease.getLeaseholderId(),
                        expiredLease.getLeaseholder(),
                        expiredLease.getStartTime()
                )
        );

        CompletableFuture<Void> prev = expirationFutureByGroup.put(grpId, eventFuture);

        assert prev == null || prev.isDone() : "Previous lease expiration process has not completed yet [grpId=" + grpId + ']';

        return eventFuture;
    }

    private CompletableFuture<Void> firePrimaryReplicaElectedEvent(long causalityToken, Lease lease) {
        UUID leaseholderId = lease.getLeaseholderId();

        assert leaseholderId != null : lease;

        return fireEvent(
                PRIMARY_REPLICA_ELECTED,
                new PrimaryReplicaEventParameters(
                        causalityToken,
                        lease.replicationGroupId(),
                        leaseholderId,
                        lease.getLeaseholder(),
                        lease.getStartTime()
                )
        );
    }

    /**
     * Determines whether the {@link PrimaryReplicaEvent#PRIMARY_REPLICA_EXPIRED} event is needed to be produced.
     *
     * @param previousLease Previous group lease, {@code null} if absent.
     * @param newLease New group lease, {@code null} if absent.
     */
    private static boolean needToFirePrimaryReplicaExpiredEvent(@Nullable Lease previousLease, @Nullable Lease newLease) {
        return isAccepted(previousLease) && (newLease == null || !isSameLease(previousLease, newLease));
    }

    /**
     * Determines whether the {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} event is needed to be produced.
     *
     * @param previousLease Previous group lease, {@code null} if absent.
     * @param newLease New group lease, {@code null} if absent.
     */
    private static boolean needToFirePrimaryReplicaElectedEvent(@Nullable Lease previousLease, @Nullable Lease newLease) {
        return isAccepted(newLease) && (!isAccepted(previousLease) || !isSameLease(previousLease, newLease));
    }

    private static boolean isSameLease(Lease previousLease, Lease newLease) {
        return previousLease.getStartTime().equals(newLease.getStartTime());
    }

    private static boolean isAccepted(@Nullable Lease lease) {
        return lease != null && lease.isAccepted();
    }
}
