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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_PREFIX;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;

/**
 * Class tracks cluster leases in memory.
 * At first, the class state recoveries from Vault, then updates on watch's listener.
 */
public class LeaseTracker {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseTracker.class);

    /** Vault manager. */
    private final VaultManager vaultManager;

    /** Meta storage manager. */
    private final MetaStorageManager msManager;

    /** Leases cache. */
    private final Map<ReplicationGroupId, Lease> leases;

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

        this.leases = new ConcurrentHashMap<>();
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
            }
        }

        LOG.info("Leases cache recovered [leases={}]", leases);
    }

    /**
     * Stops the tracker.
     */
    public void stopTrack() {
        msManager.unregisterWatch(updateListener);
    }

    /**
     * Gets a lease for a particular group.
     *
     * @param grpId Replication group id.
     * @return A lease is associated with the group.
     */
    public @NotNull Lease getLease(ReplicationGroupId grpId) {
        return leases.getOrDefault(grpId, Lease.EMPTY_LEASE);
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
                } else {
                    Lease lease = ByteUtils.fromBytes(msEntry.value());

                    leases.put(grpId, lease);
                }
            }

            return completedFuture(null);
        }

        @Override
        public void onError(Throwable e) {
        }
    }
}
