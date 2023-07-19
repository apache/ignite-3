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
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.leases.Lease.EMPTY_LEASE;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.placementdriver.LeaseMeta;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingIndependentComparableValuesTracker;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;

/**
 * Class tracks cluster leases in memory.
 * At first, the class state recoveries from Vault, then updates on watch's listener.
 */
public class LeaseTracker implements PlacementDriver {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseTracker.class);

    /** Vault manager. */
    private final VaultManager vaultManager;

    /** Meta storage manager. */
    private final MetaStorageManager msManager;

    /** Busy lock to linearize service public API calls and service stop. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the tracker. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    /** Leases cache. */
    private volatile IgniteBiTuple<Map<ReplicationGroupId, Lease>, byte[]> leases;

    /** Map of primary replica waiters. */
    private final Map<ReplicationGroupId, PendingIndependentComparableValuesTracker<HybridTimestamp, LeaseMeta>> primaryReplicaWaiters;

    /** Listener to update a leases cache. */
    private final UpdateListener updateListener = new UpdateListener();

    /**
     * The constructor.
     *
     * @param vaultManager Vault manager.
     * @param msManager Meta storage manager.
     */
    public LeaseTracker(VaultManager vaultManager, MetaStorageManager msManager) {
        this.vaultManager = vaultManager;
        this.msManager = msManager;

        this.leases = new IgniteBiTuple<>(emptyMap(), new byte[0]);
        this.primaryReplicaWaiters = new ConcurrentHashMap<>();
    }

    /**
     * Recoveries state from Vault and subscribers on further updates.
     */
    public void startTrack() {
        msManager.registerPrefixWatch(PLACEMENTDRIVER_LEASES_KEY, updateListener);

        CompletableFuture<VaultEntry> entryFut = vaultManager.get(PLACEMENTDRIVER_LEASES_KEY);

        VaultEntry entry = entryFut.join();

        Map<ReplicationGroupId, Lease> leasesMap = new HashMap<>();

        byte[] leasesBytes;

        if (entry != null) {
            leasesBytes = entry.value();

            LeaseBatch leaseBatch = LeaseBatch.fromBytes(ByteBuffer.wrap(leasesBytes).order(ByteOrder.LITTLE_ENDIAN));

            leaseBatch.leases().forEach(lease -> {
                leasesMap.put(lease.replicationGroupId(), lease);
                primaryReplicaWaiters.computeIfAbsent(lease.replicationGroupId(),
                                groupId -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE))
                        .update(lease.getExpirationTime(), lease);
            });
        } else {
            leasesBytes = new byte[0];
        }

        leases = new IgniteBiTuple<>(unmodifiableMap(leasesMap), leasesBytes);

        LOG.info("Leases cache recovered [leases={}]", leases);
    }

    /**
     * Stops the tracker.
     */
    public void stopTrack() {
        if (!isStopped.compareAndSet(false, true)) {
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
    public @NotNull Lease getLease(ReplicationGroupId grpId) {
        assert leases != null : "Leases not initialized, probably the local placement driver actor hasn't started lease tracking.";

        return leases.get1().getOrDefault(grpId, EMPTY_LEASE);
    }

    /**
     * Returns collection of leases, ordered by replication group.
     *
     * @return Collection of leases.
     */
    public IgniteBiTuple<Map<ReplicationGroupId, Lease>, byte[]> leasesCurrent() {
        return leases;
    }

    /**
     * Listen lease holder updates.
     */
    private class UpdateListener implements WatchListener {
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            for (EntryEvent entry : event.entryEvents()) {
                Entry msEntry = entry.newEntry();

                byte[] leasesBytes = msEntry.value();
                Map<ReplicationGroupId, Lease> leasesMap = new HashMap<>();

                LeaseBatch leaseBatch = LeaseBatch.fromBytes(ByteBuffer.wrap(leasesBytes).order(ByteOrder.LITTLE_ENDIAN));

                Set<ReplicationGroupId> actualGroups = new HashSet<>();

                for (Lease lease : leaseBatch.leases()) {
                    ReplicationGroupId grpId = lease.replicationGroupId();
                    actualGroups.add(grpId);

                    leasesMap.put(grpId, lease);

                    primaryReplicaWaiters.computeIfAbsent(grpId, groupId -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE))
                            .update(lease.getExpirationTime(), lease);
                }

                for (Iterator<Map.Entry<ReplicationGroupId, Lease>> iterator = leasesMap.entrySet().iterator(); iterator.hasNext();) {
                    Map.Entry<ReplicationGroupId, Lease> e = iterator.next();

                    if (!actualGroups.contains(e.getKey())) {
                        iterator.remove();
                        tryRemoveTracker(e.getKey());
                    }
                }

                LeaseTracker.this.leases = new IgniteBiTuple<>(unmodifiableMap(leasesMap), leasesBytes);
            }

            return completedFuture(null);
        }

        @Override
        public void onError(Throwable e) {
        }
    }

    @Override
    public CompletableFuture<LeaseMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException("Component is stopping."));
        }
        try {
            return primaryReplicaWaiters.computeIfAbsent(groupId, id -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE))
                    .waitFor(timestamp);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<LeaseMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException("Component is stopping."));
        }

        Map<ReplicationGroupId, Lease> leasesMap = leases.get1();

        try {
            Lease lease = leasesMap.getOrDefault(replicationGroupId, EMPTY_LEASE);

            if (lease.getExpirationTime().after(timestamp)) {
                return completedFuture(lease);
            }

            return msManager.clusterTime().waitFor(timestamp).thenApply(ignored -> inBusyLock(
                    busyLock, () -> {
                        Lease lease0 = leasesMap.getOrDefault(replicationGroupId, EMPTY_LEASE);

                        if (lease0.getExpirationTime().after(timestamp)) {
                            return lease0;
                        } else {
                            return null;
                        }
                    }));
        } finally {
            busyLock.leaveBusy();
        }
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
}
