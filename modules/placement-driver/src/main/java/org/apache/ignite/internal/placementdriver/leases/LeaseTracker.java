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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_PREFIX;
import static org.apache.ignite.internal.placementdriver.leases.Lease.EMPTY_LEASE;
import static org.apache.ignite.internal.util.IgniteUtils.bytesToList;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingIndependentComparableValuesTracker;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
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
    private final Map<ReplicationGroupId, Lease> leases;

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

        this.leases = new ConcurrentSkipListMap<>(Comparator.comparing(Object::toString));
        this.primaryReplicaWaiters = new ConcurrentHashMap<>();
    }

    /**
     * Recoveries state from Vault and subscribers on further updates.
     */
    public void startTrack() {
        msManager.registerPrefixWatch(ByteArray.fromString(PLACEMENTDRIVER_PREFIX), updateListener);

        try (Cursor<VaultEntry> cursor = vaultManager.range(
                ByteArray.fromString(PLACEMENTDRIVER_PREFIX),
                ByteArray.fromString(incrementLastChar(PLACEMENTDRIVER_PREFIX))
        )) {
            for (VaultEntry entry : cursor) {
                leases.clear();
                ByteBuffer buf = ByteBuffer.wrap(entry.value()).order(ByteOrder.LITTLE_ENDIAN);
                List<Lease> renewedLeasesList = bytesToList(buf, Lease::fromBytes);
                renewedLeasesList.forEach(lease -> {
                    leases.put(lease.replicationGroupId(), lease);
                    primaryReplicaWaiters.computeIfAbsent(lease.replicationGroupId(),
                            groupId -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE))
                                    .update(lease.getExpirationTime(), lease);
                });
            }
        }

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
        return leases.getOrDefault(grpId, EMPTY_LEASE);
    }

    private static String incrementLastChar(String str) {
        char lastChar = str.charAt(str.length() - 1);

        return str.substring(0, str.length() - 1) + (char) (lastChar + 1);
    }

    public List<Lease> leasesCurrent() {
        return leases.entrySet().stream().map(Map.Entry::getValue).sorted().collect(Collectors.toList());
    }

    /**
     * Listen lease holder updates.
     */
    private class UpdateListener implements WatchListener {
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            for (EntryEvent entry : event.entryEvents()) {
                Entry msEntry = entry.newEntry();

                ByteBuffer buf = ByteBuffer.wrap(msEntry.value()).order(ByteOrder.LITTLE_ENDIAN);

                List<Lease> renewedLeasesList = LeaseBatch.fromBytes(buf).leases();

                Map<ReplicationGroupId, Lease> renewedLeases = new HashMap<>();
                renewedLeasesList.forEach(lease -> renewedLeases.put(lease.replicationGroupId(), lease));

                for (Iterator<Map.Entry<ReplicationGroupId, Lease>> iterator = leases.entrySet().iterator(); iterator.hasNext();) {
                    Map.Entry<ReplicationGroupId, Lease> e = iterator.next();

                    if (!renewedLeases.containsKey(e.getKey())) {
                        iterator.remove();
                        tryRemoveTracker(e.getKey());
                    }
                }

                renewedLeases.forEach((grpId, lease) -> {
                    leases.put(grpId, lease);

                    primaryReplicaWaiters.computeIfAbsent(grpId, groupId -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE))
                            .update(lease.getExpirationTime(), lease);
                });
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
        try {
            Lease lease = leases.get(replicationGroupId);

            if (lease.getExpirationTime().after(timestamp)) {
                return completedFuture(lease);
            }

            // TODO: https://issues.apache.org/jira/browse/IGNITE-19532 Race between meta storage safe time publication and listeners.
            return msManager.clusterTime().waitFor(timestamp).thenApply(ignored -> inBusyLock(
                    busyLock, () -> {
                        Lease lease0 = leases.get(replicationGroupId);

                        if (lease.getExpirationTime().after(timestamp)) {
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
