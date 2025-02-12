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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntSupplier;
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
import org.apache.ignite.internal.metastorage.WatchEvent;
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

    /**
     * Constructor.
     *
     * @param nodeName Local node name.
     * @param busyLock External busy lock.
     * @param metaStorageManager Meta storage manager.
     * @param catalogManager Catalog manager.
     * @param clockService Clock service.
     * @param partitionResetClosure Closure to reset partitions.
     * @param partitionDistributionResetTimeoutSupplier Supplier for partition distribution reset timeout.
     */
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
                            // Not critical because if we have no history in this map, we look into meta storage.
                            LOG.warn("Couldn't recover data nodes history for zone [id={}, historyEntry={}].", zone.id(), historyEntry);

                            continue;
                        }

                        if (missingEntry(scaleUpEntry) || missingEntry(scaleDownEntry) || missingEntry(partitionResetEntry)) {
                            LOG.error("Couldn't recover timers for zone [id={}, name={}, scaleUpEntry={}, scaleDownEntry={}, "
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
                        if (dataNodeHistoryContext == null) {
                            // This means that the zone was not initialized yet. The initial history entry with current topology will
                            // be written on the zone init.
                            return null;
                        }

                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
                            return null;
                        }

                        LOG.debug("Topology change detected [zoneId={}, timestamp={}, newTopology={}, oldTopology={}].", zoneId, timestamp,
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

                        DistributionZoneTimer mergedScaleUpTimer = mergeTimerOnTopologyChange(zoneDescriptor, timestamp,
                                scaleUpTimer, addedNodes, newLogicalTopology, true);

                        DistributionZoneTimer mergedScaleDownTimer = mergeTimerOnTopologyChange(zoneDescriptor, timestamp,
                                scaleDownTimer, removedNodes, newLogicalTopology, false);

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
                                        addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, currentDataNodes.getFirst(),
                                                currentDataNodes.getSecond(), addMandatoryEntry),
                                        renewTimer(zoneScaleUpTimerKey(zoneId), scaleUpTimerToSave),
                                        renewTimer(zoneScaleDownTimerKey(zoneId), scaleDownTimerToSave)
                                ).yield(true),
                                ops().yield(false)
                        ),
                        updateHistoryLogRecord(zoneId, "topology change", dataNodesHistory, timestamp, currentDataNodes.getFirst(),
                                currentDataNodes.getSecond(), scaleUpTimerToSave, scaleDownTimerToSave, addMandatoryEntry),
                        "Failed to update data nodes history and timers on topology change [timestamp=" + timestamp + "]."
                        );
                    });
        }, zoneDescriptor);
    }

    private static DistributionZoneTimer mergeTimerOnTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DistributionZoneTimer currentTimer,
            Set<NodeWithAttributes> nodes,
            Set<NodeWithAttributes> logicalTopology,
            boolean scaleUp
    ) {
        DistributionZoneTimer timer;

        // Filter the current timer's nodes according to the current topology, if it is newer than the timer's timestamp.
        Set<NodeWithAttributes> currentTimerFilteredNodes = currentTimer.nodes().stream()
                .filter(n -> {
                    if (currentTimer.createTimestamp().longValue() >= timestamp.longValue()) {
                        return true;
                    } else {
                        return scaleUp == nodeNames(logicalTopology).contains(n.nodeName());
                    }
                })
                .collect(toSet());

        if (nodes.isEmpty()) {
            timer = new DistributionZoneTimer(
                    currentTimer.createTimestamp(),
                    currentTimer.timeToWaitInSeconds(),
                    currentTimerFilteredNodes
            );
        } else {
            int autoAdjustWaitInSeconds = scaleUp
                    ? zoneDescriptor.dataNodesAutoAdjustScaleUp()
                    : zoneDescriptor.dataNodesAutoAdjustScaleDown();

            timer = new DistributionZoneTimer(
                    timestamp,
                    autoAdjustWaitInSeconds,
                    union(nodes, currentTimerFilteredNodes)
            );
        }

        LOG.info("Merging scale " + (scaleUp ? "up" : "down") + " timer on topology change [zoneId={}, timestamp={}, nodes={}, topology={},"
                        + " currentTimer={}, newTimer={}].",
                zoneDescriptor.id(), timestamp, nodeNames(nodes), nodeNames(logicalTopology), currentTimer, timer);

        return timer;
    }

    /**
     * Returns timer value to save in the meta storage. If the timer is already applied according to current timestamp,
     * returns default timer.
     *
     * @param currentTimestamp Current timestamp.
     * @param timer Timer.
     * @return Timer to save.
     */
    private static DistributionZoneTimer timerToSave(HybridTimestamp currentTimestamp, DistributionZoneTimer timer) {
        return timer.timeToTrigger().longValue() <= currentTimestamp.longValue() ? DEFAULT_TIMER : timer;
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
                        assert dataNodeHistoryContext != null : "Data nodes history and timers are missing, zone=" + zoneDescriptor;

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
                            updateHistoryLogRecord(zoneId, "distribution zone filter change", dataNodesHistory, timestamp, timestamp,
                                    dataNodes, DEFAULT_TIMER, DEFAULT_TIMER),
                            "Failed to update data nodes history and timers on distribution zone filter change [timestamp="
                                    + timestamp + "]."
                            );
                    });
        }, zoneDescriptor);
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
                        assert dataNodeHistoryContext != null : "Data nodes history and timers are missing, zone=" + zoneDescriptor;

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
                                        addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, currentDataNodes.getFirst(),
                                                currentDataNodes.getSecond()),
                                        renewTimer(zoneScaleUpTimerKey(zoneId), scaleUpTimerToSave),
                                        renewTimer(zoneScaleDownTimerKey(zoneId), scaleDownTimerToSave)
                                ).yield(true),
                                ops().yield(false)
                        ),
                        updateHistoryLogRecord(zoneId, "distribution zone auto adjust change", dataNodesHistory, timestamp,
                                currentDataNodes.getFirst(), currentDataNodes.getSecond(), scaleUpTimerToSave, scaleDownTimerToSave),
                        "Failed to update data nodes history and timers on distribution zone auto adjust change [timestamp="
                                + timestamp + "]."
                        );
                    });
        }, zoneDescriptor);
    }

    private static String updateHistoryLogRecord(
            int zoneId,
            String operation,
            DataNodesHistory currentHistory,
            HybridTimestamp currentTimestamp,
            HybridTimestamp historyEntryTimestamp,
            Set<NodeWithAttributes> nodes,
            @Nullable DistributionZoneTimer scaleUpTimer,
            @Nullable DistributionZoneTimer scaleDownTimer
    ) {
        return updateHistoryLogRecord(zoneId, operation, currentHistory, currentTimestamp, historyEntryTimestamp, nodes,
                scaleUpTimer, scaleDownTimer, false);
    }

    private static String updateHistoryLogRecord(
            int zoneId,
            String operation,
            DataNodesHistory currentHistory,
            HybridTimestamp currentTimestamp,
            HybridTimestamp historyEntryTimestamp,
            Set<NodeWithAttributes> nodes,
            @Nullable DistributionZoneTimer scaleUpTimer,
            @Nullable DistributionZoneTimer scaleDownTimer,
            boolean addMandatoryEntry
    ) {
        Set<NodeWithAttributes> latestNodesWritten = currentHistory.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).getSecond();
        boolean historyEntryAdded = addMandatoryEntry || currentHistory.isEmpty() || !nodes.equals(latestNodesWritten);

        return "Updated data nodes on " + operation + ", "
                + (historyEntryAdded ? "added history entry" : "history entry not added")
                + " [zoneId=" + zoneId + ", currentTimestamp=" + currentTimestamp
                + (historyEntryAdded ? ", historyEntryTimestamp=" + historyEntryTimestamp + ", nodes=" + nodeNames(nodes) : "")
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

    private Runnable applyTimerClosure(CatalogZoneDescriptor zoneDescriptor, ScheduledTimer scheduledTimer) {
        int zoneId = zoneDescriptor.id();

        return () -> msInvokeWithRetry(msGetter -> {
            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId), scheduledTimer.metaStorageKey()))
                    .thenCompose(dataNodeHistoryContext -> {
                        assert dataNodeHistoryContext != null : "Data nodes history and timers are missing, zone=" + zoneDescriptor;

                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        DistributionZoneTimer timer = scheduledTimer.timerFromContext(dataNodeHistoryContext);

                        if (timer.equals(DEFAULT_TIMER)) {
                            return null;
                        }

                        LOG.info("Triggered " + scheduledTimer.name() + " [zoneId={}, timer={}].", zoneId, timer);

                        HybridTimestamp timeToTrigger = timer.timeToTrigger();

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timeToTrigger)) {
                            return null;
                        }

                        long currentDelay = delayInSeconds(timeToTrigger);

                        if (currentDelay < 0) {
                            return null;
                        }

                        if (currentDelay > 1) {
                            scheduledTimer.reschedule(currentDelay, applyTimerClosure(zoneDescriptor, scheduledTimer));

                            return null;
                        }

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = scheduledTimer
                                .recalculateDataNodes(dataNodesHistory, timer);

                        return clockService.waitFor(timeToTrigger.addPhysicalTime(clockService.maxClockSkewMillis()))
                                .thenApply(ignored -> new LoggedIif(iif(
                                                and(
                                                        dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                                        timerEqualToOrNotExists(scheduledTimer.metaStorageKey(), timer)
                                                ),
                                                ops(
                                                        addNewEntryToDataNodesHistory(
                                                                zoneId,
                                                                dataNodesHistory,
                                                                timeToTrigger,
                                                                currentDataNodes.getSecond()
                                                        ),
                                                        clearTimer(scheduledTimer.metaStorageKey())
                                                ).yield(true),
                                                ops().yield(false)
                                        ),
                                        updateHistoryLogRecord(zoneId, scheduledTimer.name() + " trigger", dataNodesHistory,
                                                timeToTrigger, timeToTrigger, currentDataNodes.getSecond(),
                                                scheduledTimer.scaleUpTimerAfterApply(), scheduledTimer.scaleDownTimerAfterApply()),
                                        "Failed to update data nodes history and timers on " + scheduledTimer.name() + " [timestamp="
                                                + timer.timeToTrigger() + "]."
                                ));

                    });
        }, zoneDescriptor);
    }

    private void onScaleUpTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleUpTimer) {
        ScheduledTimer timer = new ScaleUpScheduledTimer(zoneDescriptor);
        timer.init(scaleUpTimer);
    }

    private void onScaleDownTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleDownTimer) {
        ScheduledTimer timer = new ScaleDownScheduledTimer(zoneDescriptor);
        timer.init(scaleDownTimer);
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

    private void reschedulePartitionReset(long delayInSeconds, Runnable runnable, int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, this::createZoneTimers).partitionReset.reschedule(delayInSeconds, runnable);
    }

    private ZoneTimers createZoneTimers(int zoneId) {
        return new ZoneTimers(zoneId, executor);
    }

    /**
     * Calculates current data nodes on topology changes and distribution zone changes, according to the timers' state.
     * This method is NOT used for data nodes calculation for external components, for that see {@link #dataNodes(int, HybridTimestamp)}.
     *
     * @param timestamp Current timestamp.
     * @param dataNodesHistory Data nodes history.
     * @param scaleUpTimer Scale up timer.
     * @param scaleDownTimer Scale down timer.
     * @param zoneDescriptor Zone descriptor.
     * @return Pair of current timestamp and data nodes.
     */
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

    /**
     * Returns data nodes for the given zone and timestamp. See {@link #dataNodes(int, HybridTimestamp)}.
     * Catalog version is calculated by the given timestamp.
     *
     * @param zoneId Zone ID.
     * @param timestamp Timestamp.
     * @return Data nodes.
     */
    public CompletableFuture<Set<String>> dataNodes(int zoneId, HybridTimestamp timestamp) {
        return dataNodes(zoneId, timestamp, null);
    }

    /**
     * Returns data nodes for the given zone and timestamp.
     *
     * @param zoneId Zone ID.
     * @param timestamp Timestamp.
     * @param catalogVersion Catalog version.
     * @return Data nodes.
     */
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
                .thenApply(history -> inBusyLock(busyLock, () -> {
                    if (history == null) {
                        // It means that the zone was created but the data nodes value had not been updated yet.
                        // So the data nodes value will be equals to the logical topology.
                        return filterDataNodes(topologyNodes(), zone);
                    }

                    Pair<HybridTimestamp, Set<NodeWithAttributes>> entry = history.dataNodesForTimestamp(timestamp);

                    return entry.getSecond();
                }))
                .thenApply(nodes -> nodes.stream().map(NodeWithAttributes::nodeName).collect(toSet()));
    }

    private Set<NodeWithAttributes> topologyNodes() {
        // It means that the zone was created but the data nodes value had not been updated yet.
        // So the data nodes value will be equals to the logical topology on the descLastUpdateRevision.
        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey());

        if (topologyEntry.empty()) {
            // A special case for a very first start of a node, when a creation of a zone was before the first topology event,
            // meaning that zonesLogicalTopologyKey() has not been updated yet, safe to return empty set here.
            return emptySet();
        }

        return deserializeLogicalTopologySet(topologyEntry.value());
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
            Function<DataNodeHistoryContextMetaStorageGetter, CompletableFuture<LoggedIif>> iifSupplier,
            CatalogZoneDescriptor zone
    ) {
        return msInvokeWithRetry(iifSupplier, MAX_ATTEMPTS_ON_RETRY, zone);
    }

    private CompletableFuture<Void> msInvokeWithRetry(
            Function<DataNodeHistoryContextMetaStorageGetter, CompletableFuture<LoggedIif>> iifSupplier,
            int attemptsLeft,
            CatalogZoneDescriptor zone
    ) {
        if (attemptsLeft <= 0) {
            throw new AssertionError("Failed to perform meta storage invoke, maximum number of attempts reached [zone=" + zone + "].");
        }

        // Get locally on the first attempt, otherwise it means that invoke has failed because of different value in meta storage,
        // so we need to retrieve the value from meta storage.
        DataNodeHistoryContextMetaStorageGetter msGetter = attemptsLeft == MAX_ATTEMPTS_ON_RETRY
                ? this::getDataNodeHistoryContextMsLocally
                : this::getDataNodeHistoryContextMs;

        CompletableFuture<LoggedIif> iifFuture = iifSupplier.apply(msGetter);

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
                            return msInvokeWithRetry(iifSupplier, attemptsLeft - 1, zone);
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
                                    "Failed to initialize zone's dataNodes history [zoneId = {}, timestamp = {}, dataNodes = {}]",
                                    e,
                                    zoneId,
                                    timestamp,
                                    nodeNames(dataNodes)
                            );
                        } else if (invokeResult) {
                            LOG.info("Initialized zone's dataNodes history [zoneId = {}, timestamp = {}, dataNodes = {}]",
                                    zoneId,
                                    timestamp,
                                    nodeNames(dataNodes)
                            );
                        } else {
                            LOG.debug(
                                    "Failed to initialize zone's dataNodes history [zoneId = {}, timestamp = {}, dataNodes = {}]",
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
        return event -> inBusyLockAsync(
                busyLock,
                () -> processWatchEvent(
                        event,
                        DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX,
                        (zoneId, e) -> processTimerWatchEvent(zoneId, e, true)
                )
        );
    }

    private WatchListener createScaleDownTimerPrefixListener() {
        return event -> inBusyLockAsync(
                busyLock,
                () -> processWatchEvent(
                        event,
                        DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX,
                        (zoneId, e) -> processTimerWatchEvent(zoneId, e, false)
                )
        );
    }

    private WatchListener createDataNodesListener() {
        return event -> inBusyLockAsync(
                busyLock,
                () -> processWatchEvent(event, DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX, this::processDataNodesHistoryWatchEvent)
        );
    }

    private CompletableFuture<Void> processTimerWatchEvent(int zoneId, Entry e, boolean scaleUp) {
        DistributionZoneTimer timer = DistributionZoneTimerSerializer.deserialize(e.value());

        CatalogZoneDescriptor zoneDescriptor = catalogManager.activeCatalog(e.timestamp().longValue()).zone(zoneId);

        if (zoneDescriptor == null) {
            return nullCompletedFuture();
        }

        if (scaleUp) {
            onScaleUpTimerChange(zoneDescriptor, timer);
        } else {
            onScaleDownTimerChange(zoneDescriptor, timer);
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> processDataNodesHistoryWatchEvent(int zoneId, Entry e) {
        if (!e.tombstone() && e.value() != null) {
            DataNodesHistory history = DataNodesHistorySerializer.deserialize(e.value());

            CatalogZoneDescriptor zoneDescriptor = catalogManager.activeCatalog(e.timestamp().longValue()).zone(zoneId);

            if (zoneDescriptor == null) {
                return nullCompletedFuture();
            }

            dataNodesHistoryVolatile.put(zoneId, history);
        } else if (e.tombstone()) {
            dataNodesHistoryVolatile.remove(zoneId);
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> processWatchEvent(
            WatchEvent event,
            String keyPrefix,
            BiFunction<Integer, Entry, CompletableFuture<Void>> processor
    ) {
        EntryEvent entryEvent = event.entryEvent();

        Entry e = entryEvent.newEntry();

        if (e != null && !e.empty()) {
            String keyString = new String(e.key(), UTF_8);
            assert keyString.startsWith(keyPrefix) : "Unexpected key: " + keyString;

            int zoneId = extractZoneId(e.key(), keyPrefix.getBytes(UTF_8));

            return processor.apply(zoneId, e);
        }

        return nullCompletedFuture();
    }

    @TestOnly
    public ZoneTimers zoneTimers(int zoneId) {
        return zoneTimers.computeIfAbsent(zoneId, k -> new ZoneTimers(zoneId, executor));
    }

    /**
     * Representation of zone timer schedule, visible for testing purposes.
     */
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

    /**
     * Zone timers class, visible for testing purposes.
     */
    @VisibleForTesting
    public static class ZoneTimers {
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
            partitionReset.stopTimer();
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
        /** The future may contain {@code null}. */
        CompletableFuture<DataNodeHistoryContext> get(List<ByteArray> keys);
    }

    private interface ScheduledTimer {
        /**
         * Name of the timer, is used for logging.
         */
        String name();

        void init(DistributionZoneTimer timer);

        ByteArray metaStorageKey();

        DistributionZoneTimer timerFromContext(DataNodeHistoryContext context);

        void reschedule(long delayInSeconds, Runnable runnable);

        Pair<HybridTimestamp, Set<NodeWithAttributes>> recalculateDataNodes(DataNodesHistory dataNodesHistory, DistributionZoneTimer timer);

        @Nullable
        DistributionZoneTimer scaleUpTimerAfterApply();

        @Nullable
        DistributionZoneTimer scaleDownTimerAfterApply();
    }

    private class ScaleUpScheduledTimer implements ScheduledTimer {
        final CatalogZoneDescriptor zone;

        private ScaleUpScheduledTimer(CatalogZoneDescriptor zone) {
            this.zone = zone;
        }

        @Override
        public void init(DistributionZoneTimer timer) {
            int zoneId = zone.id();

            if (timer.equals(DEFAULT_TIMER)) {
                // Stop the scheduled task if the timer is default.
                zoneTimers.computeIfAbsent(zoneId, k -> createZoneTimers(zone.id())).scaleUp.stopScheduledTask();

                return;
            }

            reschedule(delayInSeconds(timer.timeToTrigger()), applyTimerClosure(zone, this));
        }

        /**
         * Name of the timer, is used for logging.
         */
        @Override
        public String name() {
            return "scale up timer";
        }

        @Override
        public ByteArray metaStorageKey() {
            return zoneScaleUpTimerKey(zone.id());
        }

        @Override
        public DistributionZoneTimer timerFromContext(DataNodeHistoryContext context) {
            return context.scaleUpTimer();
        }

        @Override
        public void reschedule(long delayInSeconds, Runnable runnable) {
            zoneTimers.computeIfAbsent(zone.id(), k -> createZoneTimers(zone.id())).scaleUp.reschedule(delayInSeconds, runnable);
        }

        @Override
        public Pair<HybridTimestamp, Set<NodeWithAttributes>> recalculateDataNodes(
                DataNodesHistory dataNodesHistory,
                DistributionZoneTimer timer
        ) {
            return currentDataNodes(timer.timeToTrigger(), dataNodesHistory, timer, DEFAULT_TIMER, zone);
        }

        @Override
        public @Nullable DistributionZoneTimer scaleUpTimerAfterApply() {
            return DEFAULT_TIMER;
        }

        @Override
        public @Nullable DistributionZoneTimer scaleDownTimerAfterApply() {
            return null;
        }
    }

    private class ScaleDownScheduledTimer implements ScheduledTimer {
        final CatalogZoneDescriptor zone;

        private ScaleDownScheduledTimer(CatalogZoneDescriptor zone) {
            this.zone = zone;
        }

        @Override
        public void init(DistributionZoneTimer timer) {
            int zoneId = zone.id();

            if (timer.equals(DEFAULT_TIMER)) {
                // Stop the scheduled task if the timer is default.
                zoneTimers.computeIfAbsent(zoneId, k -> createZoneTimers(zone.id())).scaleDown.stopScheduledTask();

                return;
            }

            reschedule(delayInSeconds(timer.timeToTrigger()), applyTimerClosure(zone, this));
        }

        /**
         * Name of the timer, is used for logging.
         */
        @Override
        public String name() {
            return "scale down timer";
        }

        @Override
        public ByteArray metaStorageKey() {
            return zoneScaleDownTimerKey(zone.id());
        }

        @Override
        public DistributionZoneTimer timerFromContext(DataNodeHistoryContext context) {
            return context.scaleDownTimer();
        }

        @Override
        public void reschedule(long delayInSeconds, Runnable runnable) {
            zoneTimers.computeIfAbsent(zone.id(), k -> createZoneTimers(zone.id())).scaleDown.reschedule(delayInSeconds, runnable);
        }

        @Override
        public Pair<HybridTimestamp, Set<NodeWithAttributes>> recalculateDataNodes(
                DataNodesHistory dataNodesHistory,
                DistributionZoneTimer timer
        ) {
            return currentDataNodes(timer.timeToTrigger(), dataNodesHistory, DEFAULT_TIMER, timer, zone);
        }

        @Override
        public @Nullable DistributionZoneTimer scaleUpTimerAfterApply() {
            return null;
        }

        @Override
        public @Nullable DistributionZoneTimer scaleDownTimerAfterApply() {
            return DEFAULT_TIMER;
        }
    }
}
