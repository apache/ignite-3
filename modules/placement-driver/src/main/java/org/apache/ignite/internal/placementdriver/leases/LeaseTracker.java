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
import static org.apache.ignite.internal.hlc.HybridTimestamp.MAX_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_PREFIX;
import static org.apache.ignite.internal.placementdriver.leases.Lease.EMPTY_LEASE;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
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

    /** Long lease interval in {@code TimeUnit.MILLISECONDS}. The interval is used between lease granting attempts. */
    private final long longLeaseInterval;

    /** Busy lock to protect {@link PlacementDriver} methods on corresponding manager stop. */
    private final IgniteSpinBusyLock busyLock;

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
     * @param longLeaseInterval Long lease interval in {@code TimeUnit.MILLISECONDS}.  The interval is used between lease granting attempts.
     * @param busyLock Busy lock to protect {@link PlacementDriver} methods on corresponding manager stop.
     */
    public LeaseTracker(
            VaultManager vaultManager,
            MetaStorageManager msManager,
            long longLeaseInterval,
            IgniteSpinBusyLock busyLock
    ) {
        this.vaultManager = vaultManager;
        this.msManager = msManager;
        this.longLeaseInterval = longLeaseInterval;
        this.busyLock = busyLock;

        this.leases = new ConcurrentHashMap<>();
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
                String key = entry.key().toString();

                key = key.replace(PLACEMENTDRIVER_PREFIX, "");

                TablePartitionId grpId = TablePartitionId.fromString(key);
                Lease lease = ByteUtils.fromBytes(entry.value());

                leases.put(grpId, lease);

                assert lease.getExpirationTime() != MAX_VALUE : "INFINITE lease expiration time isn't expected";

                primaryReplicaWaiters.computeIfAbsent(grpId, groupId -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE)).
                        update(lease.getExpirationTime(), lease);
            }
        }

        LOG.info("Leases cache recovered [leases={}]", leases);
    }

    /**
     * Stops the tracker.
     */
    public void stopTrack() {
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

    /**
     * Listen lease holder updates.
     */
    private class UpdateListener implements WatchListener {
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            for (EntryEvent entry : event.entryEvents()) {
                Entry msEntry = entry.newEntry();

                String key = new ByteArray(msEntry.key()).toString();

                key = key.replace(PLACEMENTDRIVER_PREFIX, "");

                TablePartitionId grpId = TablePartitionId.fromString(key);

                if (msEntry.empty()) {
                    leases.remove(grpId);
                    tryRemoveTracker(grpId);
                } else {
                    Lease lease = ByteUtils.fromBytes(msEntry.value());

                    assert lease.getExpirationTime() != MAX_VALUE : "INFINITE lease expiration time isn't expected";

                    leases.put(grpId, lease);

                    primaryReplicaWaiters.computeIfAbsent(grpId, groupId -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE)).
                            update(lease.getExpirationTime(), lease);
                }
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
            return primaryReplicaWaiters.computeIfAbsent(groupId, id -> new PendingIndependentComparableValuesTracker<>(MIN_VALUE)).
                    waitFor(timestamp);
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
            // There's no sense in awaiting previously detected primary replica more than lease interval.
            return awaitPrimaryReplica(replicationGroupId, timestamp).orTimeout(longLeaseInterval, TimeUnit.MILLISECONDS);
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
