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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.server.NotificationEnqueuedListener;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.jetbrains.annotations.TestOnly;

/**
 * Default implementation of {@link SchemaSafeTimeTracker}.
 */
public class SchemaSafeTimeTrackerImpl implements SchemaSafeTimeTracker, IgniteComponent, NotificationEnqueuedListener {
    private final ClusterTime clusterTime;

    private final PendingComparableValuesTracker<HybridTimestamp, Void> schemaSafeTime =
            new PendingComparableValuesTracker<>(HybridTimestamp.MIN_VALUE);

    private CompletableFuture<Void> schemaSafeTimeUpdateFuture = nullCompletedFuture();

    private final Object futureMutex = new Object();

    public SchemaSafeTimeTrackerImpl(ClusterTime clusterTime) {
        this.clusterTime = clusterTime;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        schemaSafeTime.update(clusterTime.currentSafeTime(), null);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        schemaSafeTime.close(new NodeStoppingException());

        return nullCompletedFuture();
    }

    @Override
    public void onEnqueued(CompletableFuture<Void> newNotificationFuture, List<Entry> entries, HybridTimestamp timestamp) {
        boolean hasCatalogUpdates = hasCatalogUpdates(entries);

        synchronized (futureMutex) {
            CompletableFuture<Void> newSchemaSafeTimeUpdateFuture;
            if (hasCatalogUpdates) {
                // The update touches the Catalog (i.e. schemas), so we must chain with the core notification future
                // as Catalog listeners will be included in it (because we need to wait for those listeners to finish execution
                // before updating the schema safe time).
                newSchemaSafeTimeUpdateFuture = schemaSafeTimeUpdateFuture.thenCompose(unused -> newNotificationFuture);
            } else {
                // The update does not concern the Catalog (schemas), so we can update schema safe time as soon as previous updates to it
                // get applied.
                newSchemaSafeTimeUpdateFuture = schemaSafeTimeUpdateFuture;
            }

            newSchemaSafeTimeUpdateFuture = newSchemaSafeTimeUpdateFuture.thenRun(() -> {
                schemaSafeTime.update(timestamp, null);
            });

            schemaSafeTimeUpdateFuture = newSchemaSafeTimeUpdateFuture;
        }
    }

    private static boolean hasCatalogUpdates(List<Entry> entries) {
        for (Entry entry : entries) {
            if (isCatalogUpdateEntry(entry)) {
                return true;
            }
        }

        return false;
    }

    private static boolean isCatalogUpdateEntry(Entry entry) {
        return isPrefixedWith(UpdateLogImpl.CATALOG_UPDATE_PREFIX.bytes(), entry.key());
    }

    private static boolean isPrefixedWith(byte[] prefix, byte[] key) {
        if (key.length < prefix.length) {
            return false;
        }

        return Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
    }

    @Override
    public CompletableFuture<Void> waitFor(HybridTimestamp hybridTimestamp) {
        return schemaSafeTime.waitFor(hybridTimestamp);
    }

    @TestOnly
    void enqueueFuture(CompletableFuture<Void> future) {
        synchronized (futureMutex) {
            schemaSafeTimeUpdateFuture = schemaSafeTimeUpdateFuture.thenCompose(unused -> future);
        }
    }

    @TestOnly
    HybridTimestamp currentSafeTime() {
        return schemaSafeTime.current();
    }
}
