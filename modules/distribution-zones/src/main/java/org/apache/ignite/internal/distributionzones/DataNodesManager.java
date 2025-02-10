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

package org.apache.ignite.internal.distributionzones;

import static java.lang.Math.max;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.HIGH_AVAILABILITY;
import static org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DEFAULT_TIMER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodeHistoryContextFromValues;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deserializeLogicalTopologySet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.nodeNames;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonePartitionResetTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractZoneId;
import static org.apache.ignite.internal.lang.Pair.pair;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notTombstone;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DistributionZoneTimerSerializer;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DataNodeHistoryContext;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lang.Pair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Manager for data nodes of distribution zones.
 */
public class DataNodesManager {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DataNodesManager.class);

    private static final int MAX_ATTEMPTS_ON_RETRY = 100;

    private final MetaStorageManager metaStorageManager;

    private final CatalogManager catalogManager;

    private final ClockService clockService;

    /** External busy lock. */
    private final IgniteSpinBusyLock busyLock;

    /**
     * Map with zone id as a key and set of zone timers as a value.
     */
    private final Map<Integer, ZoneTimers> zoneTimers  = new ConcurrentHashMap<>();

    /** Executor for scheduling tasks for scale up and scale down processes. */
    private final StripedScheduledThreadPoolExecutor executor;

    private final String localNodeName;

    private final WatchListener scaleUpTimerPrefixListener;

    private final WatchListener scaleDownTimerPrefixListener;

    private final WatchListener dataNodesListener;

    private final Map<Integer, DataNodesHistory> dataNodesHistoryVolatile = new ConcurrentHashMap<>();

    private final BiConsumer<Long, Integer> partitionResetClosure;

    private final IntSupplier partitionDistributionResetTimeoutSupplier;

    public DataNodesManager(
            String nodeName,
            IgniteSpinBusyLock busyLock,
            MetaStorageManager metaStorageManager,
            CatalogManager catalogManager,
            ClockService clockService,
            BiConsumer<Long, Integer> partitionResetClosure,
            IntSupplier partitionDistributionResetTimeoutSupplier
    ) {
        this.metaStorageManager = metaStorageManager;
        this.catalogManager = catalogManager;
        this.clockService = clockService;
        this.localNodeName = nodeName;
        this.partitionResetClosure = partitionResetClosure;
        this.partitionDistributionResetTimeoutSupplier = partitionDistributionResetTimeoutSupplier;
        this.busyLock = busyLock;

        executor = createZoneManagerExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
                NamedThreadFactory.create(nodeName, "dst-zones-scheduler", LOG)
        );

        scaleUpTimerPrefixListener = createScaleUpTimerPrefixListener();
        scaleDownTimerPrefixListener = createScaleDownTimerPrefixListener();
        dataNodesListener = createDataNodesListener();
    }

    CompletableFuture<Void> startAsync(
            Collection<CatalogZoneDescriptor> knownZones,
            long recoveryRevision
    ) {
        metaStorageManager.registerPrefixWatch(zoneScaleUpTimerPrefix(), scaleUpTimerPrefixListener);
        metaStorageManager.registerPrefixWatch(zoneScaleDownTimerPrefix(), scaleDownTimerPrefixListener);
        metaStorageManager.registerPrefixWatch(zoneDataNodesHistoryPrefix(), dataNodesListener);

        if (knownZones.isEmpty()) {
            return nullCompletedFuture();
        }

        Set<ByteArray> allKeys = new HashSet<>();
        Map<Integer, CatalogZoneDescriptor> descriptors = new HashMap<>();

        for (CatalogZoneDescriptor zone : knownZones) {
            List<ByteArray> zoneKeys = List.of(
                    zoneDataNodesHistoryKey(zone.id()),
                    zoneScaleUpTimerKey(zone.id()),
                    zoneScaleDownTimerKey(zone.id()),
                    zonePartitionResetTimerKey(zone.id())
            );

            allKeys.addAll(zoneKeys);
            descriptors.put(zone.id(), zone);
        }

        return metaStorageManager.getAll(allKeys)
                .thenApply(entriesMap -> {
                    for (CatalogZoneDescriptor zone : descriptors.values()) {
                        Entry historyEntry = entriesMap.get(zoneDataNodesHistoryKey(zone.id()));
                        Entry scaleUpEntry = entriesMap.get(zoneScaleUpTimerKey(zone.id()));
                        Entry scaleDownEntry = entriesMap.get(zoneScaleDownTimerKey(zone.id()));
                        Entry partitionResetEntry = entriesMap.get(zonePartitionResetTimerKey(zone.id()));

                        if (missingEntry(historyEntry)) {
                            LOG.warn("Couldn't recovery data nodes history for zone [id={}, historyEntry={}].", zone.id(), historyEntry);

                            continue;
                        }

                        if (missingEntry(scaleUpEntry) || missingEntry(scaleDownEntry) || missingEntry(partitionResetEntry)) {
                            LOG.warn("Couldn't recover timers for zone [id={}, name={}, scaleUpEntry={}, scaleDownEntry={}, "
                                    + "partitionResetEntry={}", zone.id(), zone.name(), scaleUpEntry, scaleDownEntry, partitionResetEntry);

                            continue;
                        }

                        DataNodesHistory history = DataNodesHistorySerializer.deserialize(historyEntry.value());
                        dataNodesHistoryVolatile.put(zone.id(), history);

                        DistributionZoneTimer scaleUpTimer = DistributionZoneTimerSerializer.deserialize(scaleUpEntry.value());
                        DistributionZoneTimer scaleDownTimer = DistributionZoneTimerSerializer.deserialize(scaleDownEntry.value());

                        onScaleUpTimerChange(zone, scaleUpTimer);
                        onScaleDownTimerChange(zone, scaleDownTimer);
                        restorePartitionResetTimer(zone.id(), scaleDownTimer, recoveryRevision);
                    }

                    return null;
                });
    }

    private static boolean missingEntry(Entry e) {
        return e.empty() || e.tombstone();
    }

    void stop() {
        zoneTimers.forEach((k, zt) -> zt.stopAllTimers());

        shutdownAndAwaitTermination(executor, 10, SECONDS);
    }

    CompletableFuture<Void> onTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            long revision,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> newLogicalTopology,
            Set<NodeWithAttributes> oldLogicalTopology
    ) {
        return msInvokeWithRetry(msGetter -> {
            int zoneId = zoneDescriptor.id();

            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleUpTimerKey(zoneId), zoneScaleDownTimerKey(zoneId)))
                    .thenApply(dataNodeHistoryContext -> {
                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
                            return null;
                        }

                        LOG.info("Topology change detected [zoneId={}, timestamp={}, newTopology={}, oldTopology={}].", zoneId, timestamp,
                                nodeNames(newLogicalTopology), nodeNames(oldLogicalTopology));

                        DistributionZoneTimer scaleUpTimer = dataNodeHistoryContext.scaleUpTimer();
                        DistributionZoneTimer scaleDownTimer = dataNodeHistoryContext.scaleDownTimer();

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> latestDataNodes = dataNodesHistory.dataNodesForTimestamp(timestamp);

                        Set<NodeWithAttributes> addedNodes = newLogicalTopology.stream()
                                .filter(node -> !latestDataNodes.getSecond().contains(node))
                                .collect(toSet());

                        Set<NodeWithAttributes> addedNodesComparingToOldTopology = newLogicalTopology.stream()
                                .filter(node -> !oldLogicalTopology.contains(node))
                                .collect(toSet());

                        Set<NodeWithAttributes> removedNodes = latestDataNodes.getSecond().stream()
                                .filter(node -> !newLogicalTopology.contains(node) && !Objects.equals(node.nodeName(), localNodeName))
                                .filter(node -> !scaleDownTimer.nodes().contains(node))
                                .collect(toSet());

                        if ((!addedNodes.isEmpty() || !removedNodes.isEmpty())
                                && zoneDescriptor.dataNodesAutoAdjust() != INFINITE_TIMER_VALUE) {
                            // TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
                            throw new UnsupportedOperationException("Data nodes auto adjust is not supported.");
                        }

                        int partitionResetDelay = partitionDistributionResetTimeoutSupplier.getAsInt();

                        if (!removedNodes.isEmpty()
                                && zoneDescriptor.consistencyMode() == HIGH_AVAILABILITY
                                && partitionResetDelay != INFINITE_TIMER_VALUE) {
                            reschedulePartitionReset(partitionResetDelay, () -> partitionResetClosure.accept(revision, zoneId), zoneId);
                        }

                        DistributionZoneTimer mergedScaleUpTimer = newScaleUpTimerOnTopologyChange(zoneDescriptor, timestamp,
                                scaleUpTimer, addedNodes, newLogicalTopology);

                        DistributionZoneTimer mergedScaleDownTimer = newScaleDownTimerOnTopologyChange(zoneDescriptor, timestamp,
                                scaleDownTimer, removedNodes, newLogicalTopology);

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                                timestamp,
                                dataNodesHistory,
                                mergedScaleUpTimer,
                                mergedScaleDownTimer,
                                zoneDescriptor
                        );

                        DistributionZoneTimer scaleUpTimerToSave = timerToSave(timestamp, mergedScaleUpTimer);
                        DistributionZoneTimer scaleDownTimerToSave = timerToSave(timestamp, mergedScaleDownTimer);

                        boolean addMandatoryEntry = !addedNodesComparingToOldTopology.isEmpty();

                        return new LoggedIif(iif(
                                and(
                                        dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                        and(
                                                timerEqualToOrNotExists(zoneScaleUpTimerKey(zoneId), scaleUpTimer),
                                                timerEqualToOrNotExists(zoneScaleDownTimerKey(zoneId), scaleDownTimer)
                                        )
                                ),
                                ops(
                                        addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, timestamp, currentDataNodes.getSecond(),
                                                addMandatoryEntry),
                                        renewTimer(zoneScaleUpTimerKey(zoneId), scaleUpTimerToSave),
                                        renewTimer(zoneScaleDownTimerKey(zoneId), scaleDownTimerToSave)
                                ).yield(true),
                                ops().yield(false)
                        ),
                        updateHistoryLogRecord("topology change", dataNodesHistory, timestamp, zoneId, currentDataNodes.getSecond(),
                                scaleUpTimerToSave, scaleDownTimerToSave, addMandatoryEntry),
                        "Failed to update data nodes history and timers on topology change [timestamp=" + timestamp + "]."
                        );
                    });
        });
    }

    private static DistributionZoneTimer newScaleUpTimerOnTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DistributionZoneTimer currentTimer,
            Set<NodeWithAttributes> addedNodes,
            Set<NodeWithAttributes> logicalTopology
    ) {
        DistributionZoneTimer timer;

        if (addedNodes.isEmpty()) {
            timer = currentTimer;
        } else {
            timer = new DistributionZoneTimer(
                    timestamp,
                    zoneDescriptor.dataNodesAutoAdjustScaleUp(),
                    union(addedNodes, currentTimer.nodes())
            );
        }

        return filterTimerNodesWithTopology(timer, logicalTopology, true);
    }

    private static DistributionZoneTimer newScaleDownTimerOnTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DistributionZoneTimer currentTimer,
            Set<NodeWithAttributes> removedNodes,
            Set<NodeWithAttributes> logicalTopology
    ) {
        DistributionZoneTimer timer;

        if (removedNodes.isEmpty()) {
            timer = currentTimer;
        } else {
            timer = new DistributionZoneTimer(
                    timestamp,
                    zoneDescriptor.dataNodesAutoAdjustScaleDown(),
                    union(removedNodes, currentTimer.nodes())
            );
        }

        return filterTimerNodesWithTopology(timer, logicalTopology, false);
    }

    /**
     * Filters timer's nodes according to the given topology. For scale up timer there should be no nodes that are not already
     * in the current topology. For scale down there should be no nodes that are returned to current topology. This will prevent
     * stale nodes data from being applied.
     *
     * @param timer Timer.
     * @param topology Topology.
     * @param scaleUp Scale up flag. Should be {@code false} for scale down timer.
     * @return Timer with filtered nodes.
     */
    private static DistributionZoneTimer filterTimerNodesWithTopology(
            DistributionZoneTimer timer,
            Set<NodeWithAttributes> topology,
            boolean scaleUp
    ) {
        Predicate<NodeWithAttributes> filterPredicate = scaleUp
                ? n -> nodeNames(topology).contains(n.nodeName())
                : n -> !nodeNames(topology).contains(n.nodeName());

        return new DistributionZoneTimer(
                timer.createTimestamp(),
                timer.timeToWaitInSeconds(),
                timer.nodes().stream().filter(filterPredicate).collect(toSet())
        );
    }

    private static DistributionZoneTimer timerToSave(HybridTimestamp timestamp, DistributionZoneTimer timer) {
        return timer.timeToTrigger().longValue() <= timestamp.longValue() ? DEFAULT_TIMER : timer;
    }

    CompletableFuture<Void> onZoneFilterChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> logicalTopology
    ) {
        return msInvokeWithRetry(msGetter -> {
            int zoneId = zoneDescriptor.id();

            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId)))
                    .thenApply(dataNodeHistoryContext -> {
                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
                            return null;
                        }

                        Set<NodeWithAttributes> dataNodes = filterDataNodes(logicalTopology, zoneDescriptor);

                        return new LoggedIif(iif(
                                    dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                    ops(
                                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, timestamp, dataNodes),
                                            clearTimer(zoneScaleUpTimerKey(zoneId)),
                                            clearTimer(zoneScaleDownTimerKey(zoneId)),
                                            clearTimer(zonePartitionResetTimerKey(zoneId))
                                    ).yield(true),
                                    ops().yield(false)
                            ),
                            updateHistoryLogRecord("distribution zone filter change", dataNodesHistory, timestamp, zoneId, dataNodes,
                                    DEFAULT_TIMER, DEFAULT_TIMER),
                            "Failed to update data nodes history and timers on distribution zone filter change [timestamp="
                                    + timestamp + "]."
                            );
                    });
        });
    }

    CompletableFuture<Void> onAutoAdjustAlteration(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            int oldAutoAdjustScaleUp,
            int oldAutoAdjustScaleDown
    ) {
        return msInvokeWithRetry(msGetter -> {
            int zoneId = zoneDescriptor.id();

            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleUpTimerKey(zoneId), zoneScaleDownTimerKey(zoneId)))
                    .thenApply(dataNodeHistoryContext -> {
                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
                            return null;
                        }

                        DistributionZoneTimer scaleUpTimer = dataNodeHistoryContext.scaleUpTimer();
                        DistributionZoneTimer scaleDownTimer = dataNodeHistoryContext.scaleDownTimer();

                        DistributionZoneTimer modifiedScaleUpTimer = scaleUpTimer
                                .modifyTimeToWait(zoneDescriptor.dataNodesAutoAdjustScaleUp());

                        DistributionZoneTimer modifiedScaleDownTimer = scaleDownTimer
                                .modifyTimeToWait(zoneDescriptor.dataNodesAutoAdjustScaleDown());

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                                timestamp,
                                dataNodesHistory,
                                modifiedScaleUpTimer,
                                modifiedScaleDownTimer,
                                zoneDescriptor
                        );

                        DistributionZoneTimer scaleUpTimerToSave = timerToSave(timestamp, modifiedScaleUpTimer);
                        DistributionZoneTimer scaleDownTimerToSave = timerToSave(timestamp, modifiedScaleDownTimer);

                        return new LoggedIif(iif(
                                and(
                                        dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                        and(
                                                timerEqualToOrNotExists(zoneScaleUpTimerKey(zoneId), scaleUpTimer),
                                                timerEqualToOrNotExists(zoneScaleDownTimerKey(zoneId), scaleDownTimer)
                                        )
                                ),
                                ops(
                                        addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, timestamp, currentDataNodes.getSecond()),
                                        renewTimer(zoneScaleUpTimerKey(zoneId), scaleUpTimerToSave),
                                        renewTimer(zoneScaleDownTimerKey(zoneId), scaleDownTimerToSave)
                                ).yield(true),
                                ops().yield(false)
                        ),
                        updateHistoryLogRecord("distribution zone auto adjust change", dataNodesHistory, timestamp, zoneId,
                                currentDataNodes.getSecond(), scaleUpTimerToSave, scaleDownTimerToSave),
                        "Failed to update data nodes history and timers on distribution zone auto adjust change [timestamp="
                                + timestamp + "]."
                        );
                    });
        });
    }

    private static String updateHistoryLogRecord(
            String operation,
            DataNodesHistory currentHistory,
            HybridTimestamp timestamp,
            int zoneId,
            Set<NodeWithAttributes> nodes,
            @Nullable DistributionZoneTimer scaleUpTimer,
            @Nullable DistributionZoneTimer scaleDownTimer
    ) {
        return updateHistoryLogRecord(operation, currentHistory, timestamp, zoneId, nodes, scaleUpTimer, scaleDownTimer, false);
    }

    private static String updateHistoryLogRecord(
            String operation,
            DataNodesHistory currentHistory,
            HybridTimestamp timestamp,
            int zoneId,
            Set<NodeWithAttributes> nodes,
            @Nullable DistributionZoneTimer scaleUpTimer,
            @Nullable DistributionZoneTimer scaleDownTimer,
            boolean addMandatoryEntry
    ) {
        Set<NodeWithAttributes> latestNodesWritten = currentHistory.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).getSecond();
        boolean historyEntryAdded = addMandatoryEntry || currentHistory.isEmpty() || !nodes.equals(latestNodesWritten);

        return "Updated data nodes on " + operation + ", "
                + (historyEntryAdded ? "added history entry" : "history entry not added")
                + " [zoneId=" + zoneId + ", timestamp=" + timestamp
                + (historyEntryAdded ? ", nodes=" + nodeNames(nodes) : "")
                + (historyEntryAdded ? "" : ", latestNodesWritten=" + nodeNames(latestNodesWritten))
                + "]"
                + (scaleUpTimer == null ? "" : ", scaleUpTimer=" + scaleUpTimer)
                + (scaleDownTimer == null ? "" : ", scaleDownTimer=" + scaleDownTimer)
                + "].";
    }

    void onUpdatePartitionDistributionReset(
            int zoneId,
            int partitionDistributionResetTimeoutSeconds,
            Runnable taskOnReset
    ) {
        if (partitionDistributionResetTimeoutSeconds == INFINITE_TIMER_VALUE) {
            zoneTimers.computeIfAbsent(zoneId, this::createZoneTimers).partitionReset.stopScheduledTask();
        } else {
            zoneTimers.computeIfAbsent(zoneId, this::createZoneTimers).partitionReset
                    .reschedule(partitionDistributionResetTimeoutSeconds, taskOnReset);
        }
    }

    private Runnable scaleUpClosure(CatalogZoneDescriptor zoneDescriptor) {
        int zoneId = zoneDescriptor.id();

        return () -> msInvokeWithRetry(msGetter -> {
            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleUpTimerKey(zoneId)))
                    .thenCompose(dataNodeHistoryContext -> {
                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        DistributionZoneTimer scaleUpTimer = dataNodeHistoryContext.scaleUpTimer();

                        if (scaleUpTimer.equals(DEFAULT_TIMER)) {
                            return null;
                        }

                        LOG.info("Scale up timer triggered [zoneId={}, timer={}].", zoneId, scaleUpTimer);

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(scaleUpTimer.timeToTrigger())) {
                            return null;
                        }

                        long currentDelay = delayInSeconds(scaleUpTimer.timeToTrigger());

                        if (currentDelay < 0) {
                            return null;
                        }

                        if (currentDelay > 1) {
                            rescheduleScaleUp(currentDelay, scaleUpClosure(zoneDescriptor), zoneId);

                            return null;
                        }

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                                scaleUpTimer.timeToTrigger(),
                                dataNodesHistory,
                                scaleUpTimer,
                                DEFAULT_TIMER,
                                zoneDescriptor
                        );

                        return clockService.waitFor(scaleUpTimer.timeToTrigger().addPhysicalTime(clockService.maxClockSkewMillis()))
                                .thenApply(ignored -> new LoggedIif(iif(
                                                and(
                                                        dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                                        timerEqualToOrNotExists(zoneScaleUpTimerKey(zoneId), scaleUpTimer)
                                                ),
                                                ops(
                                                        addNewEntryToDataNodesHistory(
                                                                zoneId,
                                                                dataNodesHistory,
                                                                scaleUpTimer.timeToTrigger(), currentDataNodes.getSecond()
                                                        ),
                                                        clearTimer(zoneScaleUpTimerKey(zoneId))
                                                ).yield(true),
                                                ops().yield(false)
                                        ),
                                        updateHistoryLogRecord("scale up timer trigger", dataNodesHistory, scaleUpTimer.timeToTrigger(),
                                                zoneId, currentDataNodes.getSecond(), DEFAULT_TIMER, null),
                                        "Failed to update data nodes history and timers on scale up timer trigger [timestamp="
                                                + scaleUpTimer.timeToTrigger() + "]."
                                ));

                    });
        });
    }

    private void onScaleUpTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleUpTimer) {
        int zoneId = zoneDescriptor.id();

        if (scaleUpTimer.equals(DEFAULT_TIMER)) {
            stopScaleUp(zoneId);

            return;
        }

        rescheduleScaleUp(delayInSeconds(scaleUpTimer.timeToTrigger()), scaleUpClosure(zoneDescriptor), zoneId);
    }

    private Runnable scaleDownClosure(CatalogZoneDescriptor zoneDescriptor) {
        int zoneId = zoneDescriptor.id();

        return () -> msInvokeWithRetry(msGetter -> {
            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleDownTimerKey(zoneId)))
                    .thenCompose(dataNodeHistoryContext -> {
                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        DistributionZoneTimer scaleDownTimer = dataNodeHistoryContext.scaleDownTimer();

                        if (scaleDownTimer.equals(DEFAULT_TIMER)) {
                            return null;
                        }

                        long currentDelay = delayInSeconds(scaleDownTimer.timeToTrigger());

                        if (currentDelay < 0) {
                            return null;
                        }

                        if (currentDelay > 1) {
                            rescheduleScaleUp(currentDelay, scaleDownClosure(zoneDescriptor), zoneId);

                            return null;
                        }

                        LOG.debug("Scale down timer triggered [zoneId={}, timer={}].", zoneId, scaleDownTimer);

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(scaleDownTimer.timeToTrigger())) {
                            return null;
                        }

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                                scaleDownTimer.timeToTrigger(),
                                dataNodesHistory,
                                DEFAULT_TIMER,
                                scaleDownTimer,
                                zoneDescriptor
                        );

                        return clockService.waitFor(scaleDownTimer.timeToTrigger().addPhysicalTime(clockService.maxClockSkewMillis()))
                                .thenApply(ignored -> new LoggedIif(iif(
                                                and(
                                                        dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                                        timerEqualToOrNotExists(zoneScaleDownTimerKey(zoneId), scaleDownTimer)
                                                ),
                                                ops(
                                                        addNewEntryToDataNodesHistory(zoneId, dataNodesHistory,
                                                                scaleDownTimer.timeToTrigger(), currentDataNodes.getSecond()),
                                                        clearTimer(zoneScaleDownTimerKey(zoneId))
                                                ).yield(true),
                                                ops().yield(false)
                                        ),
                                        updateHistoryLogRecord("scale down timer trigger", dataNodesHistory, scaleDownTimer.timeToTrigger(),
                                                zoneId, currentDataNodes.getSecond(), null, DEFAULT_TIMER),
                                        "Failed to update data nodes history and timers on scale down timer trigger [timestamp="
                                                + scaleDownTimer.timeToTrigger() + "]."
                                ));
                    });
        });
    }

    private void onScaleDownTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleDownTimer) {
        int zoneId = zoneDescriptor.id();

        if (scaleDownTimer.equals(DEFAULT_TIMER)) {
            stopScaleDown(zoneId);

            return;
        }

        rescheduleScaleDown(delayInSeconds(scaleDownTimer.timeToTrigger()), scaleDownClosure(zoneDescriptor), zoneId);
    }

    private void restorePartitionResetTimer(int zoneId, DistributionZoneTimer scaleDownTimer, long revision) {
        if (!scaleDownTimer.equals(DEFAULT_TIMER)) {
            reschedulePartitionReset(
                    partitionDistributionResetTimeoutSupplier.getAsInt(),
                    () -> partitionResetClosure.accept(revision, zoneId),
                    zoneId
            );
        }
    }

    private static long delayInSeconds(HybridTimestamp timerTimestamp) {
        if (timerTimestamp.equals(HybridTimestamp.MIN_VALUE) || timerTimestamp.equals(HybridTimestamp.MAX_VALUE)) {
            return -1;
        }

        long currentTime = System.currentTimeMillis();

        long delayMs = timerTimestamp.getPhysical() - currentTime;

        return max(0, delayMs / 1000);
    }

    private void stopScaleUp(int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, this::createZoneTimers).scaleUp.stopScheduledTask();
    }

    private void stopScaleDown(int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, this::createZoneTimers).scaleDown.stopScheduledTask();
    }

    private void rescheduleScaleUp(long delayInSeconds, Runnable runnable, int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, this::createZoneTimers).scaleUp.reschedule(delayInSeconds, runnable);
    }

    private void rescheduleScaleDown(long delayInSeconds, Runnable runnable, int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, this::createZoneTimers).scaleDown.reschedule(delayInSeconds, runnable);
    }

    private void reschedulePartitionReset(long delayInSeconds, Runnable runnable, int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, this::createZoneTimers).partitionReset.reschedule(delayInSeconds, runnable);
    }

    private ZoneTimers createZoneTimers(int zoneId) {
        return new ZoneTimers(zoneId, executor);
    }

    private static Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes(
            HybridTimestamp timestamp,
            DataNodesHistory dataNodesHistory,
            DistributionZoneTimer scaleUpTimer,
            DistributionZoneTimer scaleDownTimer,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodesEntry = dataNodesHistory.dataNodesForTimestamp(timestamp);

        assert currentDataNodesEntry.getFirst() != HybridTimestamp.MIN_VALUE
                : "Data nodes history is missing for timestamp [zoneId=" + zoneDescriptor.id() + ", timestamp=" + timestamp + "].";

        Set<NodeWithAttributes> dataNodes = new HashSet<>(currentDataNodesEntry.getSecond());

        long sutt = scaleUpTimer.timeToTrigger().longValue();
        long sdtt = scaleDownTimer.timeToTrigger().longValue();
        long timestampLong = timestamp.longValue();
        HybridTimestamp newTimestamp = timestamp;

        if (sutt <= timestampLong) {
            newTimestamp = scaleUpTimer.timeToTrigger();
            dataNodes.addAll(filterDataNodes(scaleUpTimer.nodes(), zoneDescriptor));
        }

        if (sdtt <= timestampLong) {
            dataNodes.removeAll(scaleDownTimer.nodes());

            if (sdtt > sutt) {
                newTimestamp = scaleDownTimer.timeToTrigger();
            }
        }

        return pair(newTimestamp, dataNodes);
    }

    public CompletableFuture<Set<String>> dataNodes(int zoneId, HybridTimestamp timestamp) {
        return dataNodes(zoneId, timestamp, null);
    }

    public CompletableFuture<Set<String>> dataNodes(int zoneId, HybridTimestamp timestamp, @Nullable Integer catalogVersion) {
        DataNodesHistory volatileHistory = dataNodesHistoryVolatile.get(zoneId);
        if (volatileHistory != null && volatileHistory.entryIsPresentAtExactTimestamp(timestamp)) {
            return completedFuture(volatileHistory.dataNodesForTimestamp(timestamp)
                    .getSecond()
                    .stream()
                    .map(NodeWithAttributes::nodeName)
                    .collect(toSet())
            );
        }

        if (catalogVersion == null) {
            catalogVersion = catalogManager.activeCatalogVersion(timestamp.longValue());
        }

        CatalogZoneDescriptor zone = catalogManager.catalog(catalogVersion).zone(zoneId);

        if (zone == null) {
            return failedFuture(new DistributionZoneNotFoundException(zoneId));
        }

        return getValueFromMetaStorage(zoneDataNodesHistoryKey(zoneId), DataNodesHistorySerializer::deserialize)
                .thenCompose(history -> {
                    if (history == null) {
                        // It means that the zone was created but the data nodes value had not been updated yet.
                        // So the data nodes value will be equals to the logical topology.
                        return defaultNodesFromTopology().thenApply(topology -> filterDataNodes(topology, zone));
                    }

                    Pair<HybridTimestamp, Set<NodeWithAttributes>> entry = history.dataNodesForTimestamp(timestamp);

                    return completedFuture(entry.getSecond());
                })
                .thenApply(nodes -> nodes.stream().map(NodeWithAttributes::nodeName).collect(toSet()));
    }

    private CompletableFuture<Set<NodeWithAttributes>> defaultNodesFromTopology() {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            CompletableFuture<Entry> topologyFut = metaStorageManager.get(zonesLogicalTopologyKey());

            return topologyFut.thenApply(topologyEntry -> {
                if (topologyEntry != null && topologyEntry.value() != null) {
                    return deserializeLogicalTopologySet(topologyEntry.value());
                } else {
                    return emptySet();
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static Condition dataNodesHistoryEqualToOrNotExists(int zoneId, DataNodesHistory history) {
        return or(
                notExists(zoneDataNodesHistoryKey(zoneId)),
                value(zoneDataNodesHistoryKey(zoneId)).eq(DataNodesHistorySerializer.serialize(history))
        );
    }

    private static Condition timerEqualToOrNotExists(ByteArray timerKey, DistributionZoneTimer timer) {
        return or(
                notExists(timerKey),
                or(
                    value(timerKey).eq(DistributionZoneTimerSerializer.serialize(timer)),
                    value(timerKey).eq(DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER))
                )
        );
    }

    private Operation addNewEntryToDataNodesHistory(
            int zoneId,
            DataNodesHistory history,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> nodes
    ) {
        return addNewEntryToDataNodesHistory(zoneId, history, timestamp, nodes, false);
    }

    private Operation addNewEntryToDataNodesHistory(
            int zoneId,
            DataNodesHistory history,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> nodes,
            boolean addMandatoryEntry
    ) {
        if (!addMandatoryEntry
                && !history.isEmpty()
                && nodes.equals(history.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).getSecond())) {
            return noop();
        } else {
            DataNodesHistory newHistory = history.addHistoryEntry(timestamp, nodes);
            dataNodesHistoryVolatile.put(zoneId, newHistory);
            return put(zoneDataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(newHistory));
        }
    }

    private static Operation renewTimer(ByteArray timerKey, DistributionZoneTimer timer) {
        return put(timerKey, DistributionZoneTimerSerializer.serialize(timer));
    }

    private static Operation clearTimer(ByteArray timerKey) {
        return put(timerKey, DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER));
    }

    @Nullable
    private <T> CompletableFuture<T> getValueFromMetaStorage(ByteArray key, Function<byte[], T> deserializer) {
        return metaStorageManager.get(key).thenApply(e -> deserializeEntry(e, deserializer));
    }

    private CompletableFuture<DataNodeHistoryContext> getDataNodeHistoryContextMs(List<ByteArray> keys) {
        return metaStorageManager.getAll(new HashSet<>(keys))
                .thenApply(entries -> dataNodeHistoryContextFromValues(entries.values()));
    }

    private CompletableFuture<DataNodeHistoryContext> getDataNodeHistoryContextMsLocally(List<ByteArray> keys) {
        List<Entry> entries = metaStorageManager.getAllLocally(keys);

        return completedFuture(dataNodeHistoryContextFromValues(entries));
    }

    @Nullable
    private static <T> T deserializeEntry(@Nullable Entry e, Function<byte[], T> deserializer) {
        if (e == null || e.value() == null || e.empty() || e.tombstone()) {
            return null;
        } else {
            return deserializer.apply(e.value());
        }
    }

    private CompletableFuture<Void> msInvokeWithRetry(
            Function<DataNodeHistoryContextMetaStorageGetter, CompletableFuture<LoggedIif>> iifSupplier
    ) {
        return msInvokeWithRetry(iifSupplier, MAX_ATTEMPTS_ON_RETRY);
    }

    private CompletableFuture<Void> msInvokeWithRetry(
            Function<DataNodeHistoryContextMetaStorageGetter, CompletableFuture<LoggedIif>> iifSupplier,
            int attemptsLeft
    ) {
        if (attemptsLeft <= 0) {
            LOG.error("Failed to perform meta storage invoke, maximum number of attempts reached.",
                    new RuntimeException("Failed to perform meta storage invoke, maximum number of attempts reached."));

            return nullCompletedFuture();
        }

        CompletableFuture<LoggedIif> iifFuture = iifSupplier.apply(attemptsLeft == MAX_ATTEMPTS_ON_RETRY
                ? this::getDataNodeHistoryContextMsLocally
                : this::getDataNodeHistoryContextMs
        );

        return iifFuture
                .thenCompose(iif -> {
                    if (iif == null) {
                        return nullCompletedFuture();
                    } else {
                        return metaStorageManager.invoke(iif.iif);
                    }
                })
                .handle((v, e) -> {
                    if (e == null) {
                        if (v == null) {
                            return null;
                        } else if (v.getAsBoolean()) {
                            LOG.info(iifFuture.join().messageOnSuccess);

                            return null;
                        } else {
                            return msInvokeWithRetry(iifSupplier, attemptsLeft - 1);
                        }
                    } else {
                        LOG.warn(iifFuture.join().messageOnFailure, e);

                        return null;
                    }
                })
                .thenCompose(c -> nullCompletedFuture());
    }

    /**
     * Method initialise data nodes history value for the specified zone, also sets the
     * {@link DistributionZonesUtil#zoneScaleUpTimerKey(int)} and {@link DistributionZonesUtil#zoneScaleDownTimerKey}
     * if it passes the condition. It is called on the first creation of a zone.
     *
     * @param zoneId Unique id of a zone
     * @param timestamp Timestamp of an event that has triggered this method.
     * @param dataNodes Data nodes.
     * @return Future reflecting the completion of initialisation of zone's keys in meta storage.
     */
    CompletableFuture<Void> onZoneCreate(
            int zoneId,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> dataNodes
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            // Update data nodes for a zone only if the corresponding data nodes keys weren't initialised in ms yet.
            Condition condition = and(
                    notExists(zoneDataNodesHistoryKey(zoneId)),
                    notTombstone(zoneDataNodesHistoryKey(zoneId))
            );

            Update update = ops(
                    addNewEntryToDataNodesHistory(zoneId, new DataNodesHistory(), timestamp, dataNodes),
                    clearTimer(zoneScaleUpTimerKey(zoneId)),
                    clearTimer(zoneScaleDownTimerKey(zoneId)),
                    clearTimer(zonePartitionResetTimerKey(zoneId))
            ).yield(true);

            Iif iif = iif(condition, update, ops().yield(false));

            return metaStorageManager.invoke(iif)
                    .thenApply(StatementResult::getAsBoolean)
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Failed to update zones' dataNodes history [zoneId = {}, timestamp = {}, dataNodes = {}]",
                                    e,
                                    zoneId,
                                    timestamp,
                                    nodeNames(dataNodes)
                            );
                        } else if (invokeResult) {
                            LOG.info("Updated zones' dataNodes history [zoneId = {}, timestamp = {}, dataNodes = {}]",
                                    zoneId,
                                    timestamp,
                                    nodeNames(dataNodes)
                            );
                        } else {
                            LOG.debug(
                                    "Failed to update zones' dataNodes history [zoneId = {}, timestamp = {}, dataNodes = {}]",
                                    zoneId,
                                    timestamp,
                                    nodeNames(dataNodes)
                            );
                        }
                    }).thenCompose((ignored) -> nullCompletedFuture());
        } finally {
            busyLock.leaveBusy();
        }
    }

    void onZoneDrop(int zoneId, HybridTimestamp timestamp) {
        ZoneTimers zt = zoneTimers.remove(zoneId);
        if (zt != null) {
            zt.stopAllTimers();
        }
    }

    private WatchListener createScaleUpTimerPrefixListener() {
        return event -> inBusyLockAsync(busyLock, () -> {
            for (EntryEvent entryEvent : event.entryEvents()) {
                Entry e = entryEvent.newEntry();

                if (e != null && !e.empty() && !e.tombstone() && e.value() != null) {
                    String keyString = new String(e.key(), UTF_8);
                    assert keyString.startsWith(DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX) : "Unexpected key: " + keyString;

                    int zoneId = extractZoneId(e.key(), DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX.getBytes(UTF_8));

                    DistributionZoneTimer timer = DistributionZoneTimerSerializer.deserialize(e.value());

                    CatalogZoneDescriptor zoneDescriptor = catalogManager.activeCatalog(event.timestamp().longValue()).zone(zoneId);

                    if (zoneDescriptor == null) {
                        return nullCompletedFuture();
                    }

                    onScaleUpTimerChange(zoneDescriptor, timer);
                }
            }

            return nullCompletedFuture();
        });
    }

    private WatchListener createScaleDownTimerPrefixListener() {
        return event -> inBusyLockAsync(busyLock, () -> {
            for (EntryEvent entryEvent : event.entryEvents()) {
                Entry e = entryEvent.newEntry();

                if (e != null && !e.empty() && !e.tombstone() && e.value() != null) {
                    String keyString = new String(e.key(), UTF_8);
                    assert keyString.startsWith(DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX) : "Unexpected key: " + keyString;

                    int zoneId = extractZoneId(e.key(), DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX.getBytes(UTF_8));

                    DistributionZoneTimer timer = DistributionZoneTimerSerializer.deserialize(e.value());

                    CatalogZoneDescriptor zoneDescriptor = catalogManager.activeCatalog(event.timestamp().longValue()).zone(zoneId);

                    if (zoneDescriptor == null) {
                        return nullCompletedFuture();
                    }

                    onScaleDownTimerChange(zoneDescriptor, timer);
                }
            }

            return nullCompletedFuture();
        });
    }

    private WatchListener createDataNodesListener() {
        return event -> inBusyLockAsync(busyLock, () -> {
            for (EntryEvent entryEvent : event.entryEvents()) {
                Entry e = entryEvent.newEntry();

                if (e != null && !e.empty()) {
                    String keyString = new String(e.key(), UTF_8);
                    assert keyString.startsWith(DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX) : "Unexpected key: " + keyString;

                    int zoneId = extractZoneId(e.key(), DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX.getBytes(UTF_8));

                    if (!e.tombstone() && e.value() != null) {
                        DataNodesHistory history = DataNodesHistorySerializer.deserialize(e.value());

                        CatalogZoneDescriptor zoneDescriptor = catalogManager.activeCatalog(event.timestamp().longValue()).zone(zoneId);

                        if (zoneDescriptor == null) {
                            return nullCompletedFuture();
                        }

                        dataNodesHistoryVolatile.put(zoneId, history);
                    } else if (e.tombstone()) {
                        dataNodesHistoryVolatile.remove(zoneId);
                    }
                }
            }

            return nullCompletedFuture();
        });
    }

    @TestOnly
    public ZoneTimers zoneTimers(int zoneId) {
        return zoneTimers.computeIfAbsent(zoneId, k -> new ZoneTimers(zoneId, executor));
    }

    @VisibleForTesting
    public static class ZoneTimerSchedule {
        final StripedScheduledThreadPoolExecutor executor;
        final int zoneId;
        private @Nullable ScheduledFuture<?> taskFuture;
        private long delay;

        ZoneTimerSchedule(int zoneId, StripedScheduledThreadPoolExecutor executor) {
            this.zoneId = zoneId;
            this.executor = executor;
        }

        synchronized void reschedule(long delayInSeconds, Runnable task) {
            stopScheduledTask();

            delay = delayInSeconds;

            if (delayInSeconds >= 0) {
                taskFuture = executor.schedule(task, delayInSeconds, SECONDS, zoneId);
            }
        }

        synchronized void stopScheduledTask() {
            if (taskFuture != null && delay > 0) {
                taskFuture.cancel(false);
                taskFuture = null;

                delay = 0;
            }
        }

        synchronized void stopTimer() {
            if (taskFuture != null) {
                taskFuture.cancel(false);
                taskFuture = null;
            }
        }

        @TestOnly
        public synchronized boolean taskIsScheduled() {
            return taskFuture != null;
        }

        @TestOnly
        public synchronized boolean taskIsCancelled() {
            return taskFuture == null;
        }

        @TestOnly
        public synchronized boolean taskIsDone() {
            return taskFuture != null && taskFuture.isDone();
        }
    }

    @VisibleForTesting
    public class ZoneTimers {
        public final ZoneTimerSchedule scaleUp;
        public final ZoneTimerSchedule scaleDown;
        public final ZoneTimerSchedule partitionReset;

        ZoneTimers(int zoneId, StripedScheduledThreadPoolExecutor executor) {
            this.scaleUp = new ZoneTimerSchedule(zoneId, executor);
            this.scaleDown = new ZoneTimerSchedule(zoneId, executor);
            this.partitionReset = new ZoneTimerSchedule(zoneId, executor);
        }

        void stopAllTimers() {
            scaleUp.stopTimer();
            scaleDown.stopTimer();
            partitionReset.stopTimer() ;
        }
    }

    private static class LoggedIif {
        final Iif iif;
        final String messageOnSuccess;
        final String messageOnFailure;

        public LoggedIif(Iif iif, String messageOnSuccess, String messageOnFailure) {
            this.iif = iif;
            this.messageOnSuccess = messageOnSuccess;
            this.messageOnFailure = messageOnFailure;
        }
    }

    @FunctionalInterface
    private interface DataNodeHistoryContextMetaStorageGetter {
        CompletableFuture<DataNodeHistoryContext> get(List<ByteArray> keys);
    }
}
