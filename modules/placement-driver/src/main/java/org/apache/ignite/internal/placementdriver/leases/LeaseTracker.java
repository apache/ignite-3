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
import static org.apache.ignite.internal.hlc.HybridTimestamp.CLOCK_SKEW;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;
import static org.apache.ignite.internal.placementdriver.leases.Lease.EMPTY_LEASE;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingIndependentComparableValuesTracker;
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

    /** Listener to update a leases cache. */
    private final UpdateListener updateListener = new UpdateListener();

    /**
     * Constructor.
     *
     * @param msManager Meta storage manager.
     */
    public LeaseTracker(MetaStorageManager msManager) {
        this.msManager = msManager;
    }

    /**
     * Recovers state from Vault and subscribes to future updates.
     *
     * @param recoveryRevision Revision from {@link MetaStorageManager#recoveryFinishedFuture()}.
     */
    public CompletableFuture<Void> startTrackAsync(long recoveryRevision) {
        return inBusyLock(busyLock, () -> {
            msManager.registerPrefixWatch(PLACEMENTDRIVER_LEASES_KEY, updateListener);

            return loadLeasesBusyAsync(recoveryRevision);
        });
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

    /**
     * Gets a lease for a particular group.
     *
     * @param grpId Replication group id.
     * @return A lease is associated with the group.
     */
    public Lease getLease(ReplicationGroupId grpId) {
        Leases leases = this.leases;

        assert leases != null : "Leases not initialized, probably the local placement driver actor hasn't started lease tracking.";

        return leases.leaseByGroupId().getOrDefault(grpId, EMPTY_LEASE);
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

                for (EntryEvent entry : event.entryEvents()) {
                    Entry msEntry = entry.newEntry();

                    byte[] leasesBytes = msEntry.value();
                    Map<ReplicationGroupId, Lease> leasesMap = new HashMap<>();

                    LeaseBatch leaseBatch = LeaseBatch.fromBytes(ByteBuffer.wrap(leasesBytes).order(LITTLE_ENDIAN));

                    Set<ReplicationGroupId> actualGroups = new HashSet<>();

                    Map<ReplicationGroupId, Lease> previousLeasesMap = LeaseTracker.this.leases.leaseByGroupId();

                    for (Lease lease : leaseBatch.leases()) {
                        ReplicationGroupId grpId = lease.replicationGroupId();
                        actualGroups.add(grpId);

                        leasesMap.put(grpId, lease);

                        if (lease.isAccepted()) {
                            primaryReplicaWaiters
                                    .computeIfAbsent(grpId, groupId -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE))
                                    .update(lease.getExpirationTime(), lease);

                            if (needFireEventReplicaBecomePrimary(previousLeasesMap.get(grpId), lease)) {
                                fireEventFutures.add(fireEventReplicaBecomePrimary(event.revision(), lease));
                            }
                        }
                    }

                    for (Iterator<Map.Entry<ReplicationGroupId, Lease>> iterator = leasesMap.entrySet().iterator(); iterator.hasNext();) {
                        Map.Entry<ReplicationGroupId, Lease> e = iterator.next();

                        if (!actualGroups.contains(e.getKey())) {
                            iterator.remove();
                            tryRemoveTracker(e.getKey());
                        }
                    }

                    LeaseTracker.this.leases = new Leases(unmodifiableMap(leasesMap), leasesBytes);
                }

                return allOf(fireEventFutures.toArray(CompletableFuture[]::new));
            });
        }

        @Override
        public void onError(Throwable e) {
            LOG.warn("Unable to process update leases event", e);
        }
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp) {
        return inBusyLockAsync(busyLock, () -> getOrCreatePrimaryReplicaWaiter(groupId).waitFor(timestamp));
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        HybridTimestamp timestampWithClockSkew = timestamp.addPhysicalTime(CLOCK_SKEW);

        return inBusyLockAsync(busyLock, () -> {
            Lease lease = leases.leaseByGroupId().getOrDefault(replicationGroupId, EMPTY_LEASE);

            if (lease.isAccepted() && lease.getExpirationTime().after(timestampWithClockSkew)) {
                return completedFuture(lease);
            }

            return msManager
                    .clusterTime()
                    .waitFor(timestampWithClockSkew)
                    .thenApply(ignored -> inBusyLock(busyLock, () -> {
                        Lease lease0 = leases.leaseByGroupId().getOrDefault(replicationGroupId, EMPTY_LEASE);

                        if (lease0.isAccepted() && lease0.getExpirationTime().after(timestampWithClockSkew)) {
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

    private CompletableFuture<Void> loadLeasesBusyAsync(long recoveryRevision) {
        Entry entry = msManager.getLocally(PLACEMENTDRIVER_LEASES_KEY, recoveryRevision);

        CompletableFuture<Void> loadLeasesFuture;

        if (entry.empty() || entry.tombstone()) {
            leases = new Leases(Map.of(), BYTE_EMPTY_ARRAY);

            loadLeasesFuture = completedFuture(null);
        } else {
            byte[] leasesBytes = entry.value();

            LeaseBatch leaseBatch = LeaseBatch.fromBytes(ByteBuffer.wrap(leasesBytes).order(LITTLE_ENDIAN));

            Map<ReplicationGroupId, Lease> leasesMap = new HashMap<>();

            List<CompletableFuture<?>> fireEventFutures = new ArrayList<>();

            leaseBatch.leases().forEach(lease -> {
                leasesMap.put(lease.replicationGroupId(), lease);

                if (lease.isAccepted()) {
                    getOrCreatePrimaryReplicaWaiter(lease.replicationGroupId()).update(lease.getExpirationTime(), lease);

                    // needFireEventReplicaBecomePrimary is not needed because we need to recover the last leases.
                    fireEventFutures.add(fireEventReplicaBecomePrimary(recoveryRevision, lease));
                }
            });

            leases = new Leases(unmodifiableMap(leasesMap), leasesBytes);

            loadLeasesFuture = allOf(fireEventFutures.toArray(CompletableFuture[]::new));
        }

        LOG.info("Leases cache recovered [leases={}]", leases);

        return loadLeasesFuture;
    }

    private CompletableFuture<Void> fireEventReplicaBecomePrimary(long causalityToken, Lease lease) {
        String leaseholder = lease.getLeaseholder();

        assert leaseholder != null : lease;

        return fireEvent(
                PRIMARY_REPLICA_ELECTED,
                new PrimaryReplicaEventParameters(causalityToken, lease.replicationGroupId(), leaseholder)
        );
    }

    /**
     * Checks whether event {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} should be fired for an <b>accepted</b> lease.
     *
     * @param previousLease Previous group lease, {@code null} if absent.
     * @param newLease New group lease.
     * @return {@code true} if there is no previous lease for the group or the new lease is not prolongation.
     */
    private static boolean needFireEventReplicaBecomePrimary(@Nullable Lease previousLease, Lease newLease) {
        assert newLease.isAccepted() : newLease;

        return previousLease == null || !previousLease.getStartTime().equals(newLease.getStartTime());
    }
}
