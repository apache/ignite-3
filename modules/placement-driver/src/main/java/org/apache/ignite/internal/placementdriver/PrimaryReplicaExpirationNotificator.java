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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;

/**
 * The class notify about a local primary replica expiration.
 */
public class PrimaryReplicaExpirationNotificator {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PrimaryReplicaExpirationNotificator.class);

    /** Expiration future by replication group. */
    private final Map<ReplicationGroupId, CompletableFuture<Void>> expirationFutureByGroup = new ConcurrentHashMap<>();
    /** Mapping replica id to supplier. */
    private final Map<ReplicationGroupId, Set<Supplier<CompletableFuture<Void>>>> listeners = new ConcurrentHashMap<>();
    /** Local node. */
    private final String nodeName;

    /**
     * The constructor.
     *
     * @param nodeName Ignite node name.
     */
    public PrimaryReplicaExpirationNotificator(String nodeName) {
        this.nodeName = nodeName;
    }

    /** Stops notificator. */
    public void stop() {
        expirationFutureByGroup.values().stream().forEach(f -> f.completeExceptionally(new NodeStoppingException()));

        listeners.clear();
    }

    /**
     * Subscribes a listener to the local primary replica expiration.
     *
     * @param grpId Replication group id.
     * @param listener Primary expiration listener.
     */
    public void subscribe(ReplicationGroupId grpId, Supplier<CompletableFuture<Void>> listener) {
        listeners.computeIfAbsent(grpId, v -> new ConcurrentHashSet<>()).add(listener);
    }

    /**
     * Unsubscribes a listener to the local primary replica expiration.
     *
     * @param grpId Replication group id.
     * @param listener Primary expiration listener.
     */
    public void unsubscribe(ReplicationGroupId grpId, Supplier<CompletableFuture<Void>> listener) {
        listeners.computeIfAbsent(grpId, v -> new ConcurrentHashSet<>()).remove(listener);
    }

    /**
     * Invokes all expiration listeners for the replication group identified by a leas.
     *
     * @param expiredLease Expired lease.
     */
    public void onLeaseExpire(Lease expiredLease) {
        ReplicationGroupId grpId = expiredLease.replicationGroupId();

        if (nodeName.equals(expiredLease.getLeaseholder()) && listeners.containsKey(grpId)) {
            Set<CompletableFuture<Void>> futs = listeners.get(grpId).stream().map(Supplier::get).collect(Collectors.toSet());

            CompletableFuture<Void> prev = expirationFutureByGroup.put(grpId,
                    CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new)));

            if (prev != null && !prev.isDone()) {
                LOG.warn("Previous lease expiration process has not completed yet [grpId={}]", grpId);
            }
        }
    }

    /**
     * Gets a complete expiration future. If the future is completed the local replica can be primary again.
     *
     * @param grpId Replication group id.
     * @return Future to complete expiration.
     */
    public CompletableFuture<Void> completionFuture(ReplicationGroupId grpId) {
        CompletableFuture<Void> fut = expirationFutureByGroup.get(grpId);

        if (expirationFutureByGroup.containsKey(grpId)) {
            System.out.println();
        }

        return fut == null ? CompletableFuture.completedFuture(null) : fut;
    }
}
