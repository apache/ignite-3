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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * Class that helps tracking starting and already started replicas. The flow is controlled from the outside, this class only helps
 * maintaining collections of replicas in different statuses.
 */
class StartedReplicationGroups {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(StartedReplicationGroups.class);

    /** Groups that are starting but are not yet reported to be fully started. */
    private final Map<ZonePartitionId, CompletableFuture<Void>> startingReplicationGroupIds = new ConcurrentHashMap<>();

    /** Groups that are reported to be started. */
    private final Set<ZonePartitionId> startedReplicationGroupIds = ConcurrentHashMap.newKeySet();

    /**
     * Callback to be called before replication group begins start procedure. It is expected that whoever maintains the replication groups
     * will call either {@link #startingFailed(ZonePartitionId)} or {@link #startingCompleted(ZonePartitionId)} some time after..
     */
    void beforeStartingGroup(ZonePartitionId zonePartitionId) {
        CompletableFuture<Void> startingFuture = new CompletableFuture<>();

        CompletableFuture<Void> prevFuture = startingReplicationGroupIds.put(zonePartitionId, startingFuture);

        assert prevFuture == null : "Replication group is starting second time. [zonePartitionId=" + zonePartitionId + ']';

        // Copy future state if assertions are disabled.
        //noinspection ConstantValue
        if (prevFuture != null) {
            startingFuture.whenComplete(copyStateTo(prevFuture));
        }
    }

    /**
     * Mark starting replication groups as failed. Such a group won't be considered {@code starting} anymore.
     */
    void startingFailed(ZonePartitionId zonePartitionId) {
        completeStartingFuture(zonePartitionId);
    }

    /**
     * Mark starting replication groups as successfully started. Such a group won't be considered {@code starting} anymore. After this call
     * the {@link #streamStartedReplicationGroups()} will start returning this ID.
     */
    void startingCompleted(ZonePartitionId zonePartitionId) {
        startedReplicationGroupIds.add(zonePartitionId);

        completeStartingFuture(zonePartitionId);
    }

    /**
     * Marks the replication group as stopped. After this call the {@link #streamStartedReplicationGroups()} will stop returning this ID.
     */
    void afterStoppingGroup(ZonePartitionId zonePartitionId) {
        startedReplicationGroupIds.remove(zonePartitionId);
    }

    /**
     * Check if the replication group has started.
     *
     * @see #startingCompleted(ZonePartitionId)
     * @see #afterStoppingGroup(ZonePartitionId)
     */
    boolean hasReplicationGroupStarted(ZonePartitionId zonePartitionId) {
        return startedReplicationGroupIds.contains(zonePartitionId);
    }

    private void completeStartingFuture(ZonePartitionId zonePartitionId) {
        CompletableFuture<Void> startingFuture = startingReplicationGroupIds.remove(zonePartitionId);

        assert startingFuture != null : "Starting future is not found. [zonePartitionId=" + zonePartitionId + ']';

        //noinspection ConstantValue
        if (startingFuture != null) {
            startingFuture.complete(null);
        }
    }

    /**
     * Wait for all replication groups in {@code starting} status to call {@link #startingFailed(ZonePartitionId)} or
     * {@link #startingCompleted(ZonePartitionId)}.
     */
    void waitForStartingReplicas() {
        try {
            CompletableFutures.allOf(startingReplicationGroupIds.values()).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed to wait starting replicas before stop", e);
        }
    }

    /**
     * Returns a stream of IDs of started replication groups.
     */
    Stream<ZonePartitionId> streamStartedReplicationGroups() {
        return startedReplicationGroupIds.stream();
    }
}
