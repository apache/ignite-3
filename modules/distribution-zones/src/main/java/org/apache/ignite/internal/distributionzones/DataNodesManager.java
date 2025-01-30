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
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.HIGH_AVAILABILITY;
import static org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DEFAULT_TIMER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodeHistoryContextFromValues;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.nodeNames;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonePartitionResetTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerPrefix;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractZoneId;
import static org.apache.ignite.internal.lang.Pair.pair;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
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

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DistributionZoneTimerSerializer;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DataNodeHistoryContext;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
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

    public DataNodesManager(
            String nodeName,
            IgniteSpinBusyLock busyLock,
            MetaStorageManager metaStorageManager,
            CatalogManager catalogManager
    ) {
        this.metaStorageManager = metaStorageManager;
        this.catalogManager = catalogManager;
        this.localNodeName = nodeName;
        this.busyLock = busyLock;

        executor = createZoneManagerExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
                NamedThreadFactory.create(nodeName, "dst-zones-scheduler", LOG)
        );

        scaleUpTimerPrefixListener = createScaleUpTimerPrefixListener();
        scaleDownTimerPrefixListener = createScaleDownTimerPrefixListener();
    }

    void start(
            Collection<CatalogZoneDescriptor> knownZones
    ) {
        for (CatalogZoneDescriptor zone : knownZones) {
            onScaleUpTimerChange(zone);
            onScaleDownTimerChange(zone);
        }

        metaStorageManager.registerPrefixWatch(zoneScaleUpTimerPrefix(), scaleUpTimerPrefixListener);
        metaStorageManager.registerPrefixWatch(zoneScaleDownTimerPrefix(), scaleDownTimerPrefixListener);
    }

    void stop() {
        zoneTimers.forEach((k, zt) -> zt.stopAllTimers());

        shutdownAndAwaitTermination(executor, 10, SECONDS);
    }

    CompletableFuture<Void> onTopologyChangeZoneHandler(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> newLogicalTopology,
            int partitionResetDelay,
            Runnable taskOnPartitionReset
    ) {
        return msInvokeWithRetry(msGetter -> {
            int zoneId = zoneDescriptor.id();

            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleUpTimerKey(zoneId), zoneScaleDownTimerKey(zoneId)))
                    .thenApply(dataNodeHistoryContext -> {
                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
                            return null;
                        }

                        LOG.warn("Topology change detected [zoneId={}, timestamp={}, newTopology={}].", zoneId, timestamp, nodeNames(newLogicalTopology));

                        DistributionZoneTimer scaleUpTimer = dataNodeHistoryContext.scaleUpTimer();
                        DistributionZoneTimer scaleDownTimer = dataNodeHistoryContext.scaleDownTimer();

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                                timestamp,
                                dataNodesHistory,
                                scaleUpTimer,
                                scaleDownTimer,
                                zoneDescriptor
                        );

                        Set<NodeWithAttributes> addedNodes = newLogicalTopology.stream()
                                .filter(node -> !currentDataNodes.getSecond().contains(node))
                                .collect(toSet());

                        Set<NodeWithAttributes> removedNodes = currentDataNodes.getSecond().stream()
                                .filter(node -> !newLogicalTopology.contains(node) && !Objects.equals(node.nodeName(), localNodeName))
                                .collect(toSet());

                        if ((!addedNodes.isEmpty() || !removedNodes.isEmpty())
                                && zoneDescriptor.dataNodesAutoAdjust() != INFINITE_TIMER_VALUE) {
                            // TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
                            throw new UnsupportedOperationException("Data nodes auto adjust is not supported.");
                        }

                        if (!removedNodes.isEmpty() && zoneDescriptor.consistencyMode() == HIGH_AVAILABILITY) {
                            if (partitionResetDelay != INFINITE_TIMER_VALUE) {
                                reschedulePartitionReset(partitionResetDelay, taskOnPartitionReset, zoneId);
                            }
                        }

                        DistributionZoneTimer newScaleUpTimer = newScaleUpTimerOnTopologyChange(zoneDescriptor, timestamp,
                                scaleUpTimer, addedNodes);

                        DistributionZoneTimer newScaleDownTimer = newScaleDownTimerOnTopologyChange(zoneDescriptor, timestamp,
                                scaleDownTimer, removedNodes);

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
                                        renewTimerOrClearIfAlreadyApplied(zoneScaleUpTimerKey(zoneId), timestamp, newScaleUpTimer),
                                        renewTimerOrClearIfAlreadyApplied(zoneScaleDownTimerKey(zoneId), timestamp, newScaleDownTimer)
                                ).yield(true),
                                ops().yield(false)
                        ),
                        updateHistoryLogRecord("topology change", dataNodesHistory, timestamp, zoneId, currentDataNodes.getSecond(),
                                newScaleUpTimer, newScaleDownTimer),
                        "Failed to update data nodes history and timers on topology change [timestamp=" + timestamp + "]."
                        );

                    });
        });
    }

    private static DistributionZoneTimer newScaleUpTimerOnTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DistributionZoneTimer currentTimer,
            Set<NodeWithAttributes> addedNodes
    ) {
        if (addedNodes.isEmpty()) {
            return DEFAULT_TIMER;
        } else {
            return new DistributionZoneTimer(
                    timestamp,
                    zoneDescriptor.dataNodesAutoAdjustScaleUp(),
                    union(addedNodes, currentTimer.timeToTrigger().longValue() < timestamp.longValue() ? emptySet() : currentTimer.nodes())
            );
        }
    }

    private static DistributionZoneTimer newScaleDownTimerOnTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DistributionZoneTimer currentTimer,
            Set<NodeWithAttributes> removedNodes
    ) {
        if (removedNodes.isEmpty()) {
            return DEFAULT_TIMER;
        } else {
            return new DistributionZoneTimer(
                    timestamp,
                    zoneDescriptor.dataNodesAutoAdjustScaleDown(),
                    union(removedNodes, currentTimer.timeToTrigger().longValue() < timestamp.longValue() ? emptySet() : currentTimer.nodes())
            );
        }
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

                        DistributionZoneTimer newScaleUpTimer = scaleUpTimer
                                .modifyTimeToWait(zoneDescriptor.dataNodesAutoAdjustScaleUp());

                        DistributionZoneTimer newScaleDownTimer = scaleDownTimer
                                .modifyTimeToWait(zoneDescriptor.dataNodesAutoAdjustScaleDown());

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                                timestamp,
                                dataNodesHistory,
                                newScaleUpTimer,
                                newScaleDownTimer,
                                zoneDescriptor
                        );

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
                                        renewTimerOrClearIfAlreadyApplied(zoneScaleUpTimerKey(zoneId), timestamp, newScaleUpTimer),
                                        renewTimerOrClearIfAlreadyApplied(zoneScaleDownTimerKey(zoneId), timestamp, newScaleDownTimer)
                                ).yield(true),
                                ops().yield(false)
                        ),
                        updateHistoryLogRecord("distribution zone auto adjust change", dataNodesHistory, timestamp, zoneId,
                                currentDataNodes.getSecond(), newScaleUpTimer, newScaleDownTimer),
                        "Failed to update data nodes history and timers on distribution zone auto adjust change [timestamp="
                                + timestamp + "]."
                        );
                    });
        });
    }

    private String updateHistoryLogRecord(
            String operation,
            DataNodesHistory currentHistory,
            HybridTimestamp timestamp,
            int zoneId,
            Set<NodeWithAttributes> nodes,
            @Nullable DistributionZoneTimer scaleUpTimer,
            @Nullable DistributionZoneTimer scaleDownTimer
    ) {
        Set<NodeWithAttributes> latestNodesWritten = currentHistory.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).getSecond();
        boolean historyEntryAdded = currentHistory.isEmpty() || !nodes.equals(latestNodesWritten);

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

    private void onScaleUpTimerChange(CatalogZoneDescriptor zoneDescriptor) {
        int zoneId = zoneDescriptor.id();

        // #join here because local meta storage call returns completed future.
        DistributionZoneTimer scaleUpTimer = getDataNodeHistoryContextMsLocally(List.of(zoneScaleUpTimerKey(zoneId)))
                .thenApply(DataNodeHistoryContext::scaleUpTimer)
                .join();

        if (scaleUpTimer.equals(DEFAULT_TIMER)) {
            return;
        }

        onScaleUpTimerChange(zoneDescriptor, scaleUpTimer);
    }

    private void onScaleUpTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleUpTimer) {
        int zoneId = zoneDescriptor.id();

        if (scaleUpTimer.equals(DEFAULT_TIMER)) {
            stopScaleUp(zoneId);

            return;
        }

        Runnable runnable = () -> msInvokeWithRetry(msGetter -> {
            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleUpTimerKey(zoneId)))
                    .thenApply(dataNodeHistoryContext -> {
                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        DistributionZoneTimer scaleUpTimer0 = dataNodeHistoryContext.scaleUpTimer();

                        if (scaleUpTimer0.equals(DEFAULT_TIMER)) {
                            return null;
                        }

                        LOG.info("Scale up timer triggered [zoneId={}, timer={}].", zoneId, scaleUpTimer0);

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(scaleUpTimer0.timeToTrigger())) {
                            return null;
                        }

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                                scaleUpTimer0.timeToTrigger(),
                                dataNodesHistory,
                                scaleUpTimer0,
                                DEFAULT_TIMER,
                                zoneDescriptor
                        );

                        return new LoggedIif(iif(
                                    and(
                                            dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                            timerEqualToOrNotExists(zoneScaleUpTimerKey(zoneId), scaleUpTimer0)
                                    ),
                                    ops(
                                            addNewEntryToDataNodesHistory(
                                                    zoneId,
                                                    dataNodesHistory,
                                                    scaleUpTimer0.timeToTrigger(), currentDataNodes.getSecond()
                                            ),
                                            clearTimer(zoneScaleUpTimerKey(zoneId))
                                    ).yield(true),
                                    ops().yield(false)
                            ),
                            updateHistoryLogRecord("scale up timer trigger", dataNodesHistory, scaleUpTimer0.timeToTrigger(), zoneId,
                                    currentDataNodes.getSecond(), DEFAULT_TIMER, null),
                            "Failed to update data nodes history and timers on scale up timer trigger [timestamp="
                                    + scaleUpTimer0.timeToTrigger() + "]."
                        );
                    });
        });

        rescheduleScaleUp(delayInSeconds(scaleUpTimer.timeToTrigger()), runnable, zoneId);
    }

    private void onScaleDownTimerChange(CatalogZoneDescriptor zoneDescriptor) {
        int zoneId = zoneDescriptor.id();

        // #join here because local meta storage call returns completed future.
        DistributionZoneTimer scaleDownTimer = getDataNodeHistoryContextMsLocally(List.of(zoneScaleDownTimerKey(zoneId)))
                .thenApply(DataNodeHistoryContext::scaleDownTimer)
                .join();

        if (scaleDownTimer.equals(DEFAULT_TIMER)) {
            return;
        }

        onScaleDownTimerChange(zoneDescriptor, scaleDownTimer);
    }

    private void onScaleDownTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleDownTimer) {
        int zoneId = zoneDescriptor.id();

        if (scaleDownTimer.equals(DEFAULT_TIMER)) {
            stopScaleDown(zoneId);

            return;
        }

        Runnable runnable = () -> msInvokeWithRetry(msGetter -> {
            return msGetter.get(List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleDownTimerKey(zoneId)))
                    .thenApply(dataNodeHistoryContext -> {
                        DataNodesHistory dataNodesHistory = dataNodeHistoryContext.dataNodesHistory();

                        DistributionZoneTimer scaleDownTimer0 = dataNodeHistoryContext.scaleDownTimer();

                        if (scaleDownTimer0.equals(DEFAULT_TIMER)) {
                            return null;
                        }

                        LOG.info("Scale down timer triggered [zoneId={}, timer={}].", zoneId, scaleDownTimer0);

                        if (dataNodesHistory.entryIsPresentAtExactTimestamp(scaleDownTimer0.timeToTrigger())) {
                            return null;
                        }

                        Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                                scaleDownTimer0.timeToTrigger(),
                                dataNodesHistory,
                                DEFAULT_TIMER,
                                scaleDownTimer0,
                                zoneDescriptor
                        );

                        return new LoggedIif(iif(
                                    and(
                                            dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                            timerEqualToOrNotExists(zoneScaleDownTimerKey(zoneId), scaleDownTimer0)
                                    ),
                                    ops(
                                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory,
                                                    scaleDownTimer0.timeToTrigger(), currentDataNodes.getSecond()),
                                            clearTimer(zoneScaleDownTimerKey(zoneId))
                                    ).yield(true),
                                    ops().yield(false)
                            ),
                            updateHistoryLogRecord("scale down timer trigger", dataNodesHistory, scaleDownTimer0.timeToTrigger(), zoneId,
                                    currentDataNodes.getSecond(), null, DEFAULT_TIMER),
                            "Failed to update data nodes history and timers on scale down timer trigger [timestamp="
                                    + scaleDownTimer0.timeToTrigger() + "]."
                        );
                    });
        });

        rescheduleScaleDown(delayInSeconds(scaleDownTimer.timeToTrigger()), runnable, zoneId);
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

        long cdnt = currentDataNodesEntry.getFirst().longValue();
        long sutt = scaleUpTimer.timeToTrigger().longValue();
        long sdtt = scaleDownTimer.timeToTrigger().longValue();
        long timestampLong = timestamp.longValue();
        HybridTimestamp newTimestamp = timestamp;

        if (sutt > cdnt && sutt <= timestampLong) {
            newTimestamp = scaleUpTimer.timeToTrigger();
            dataNodes.addAll(filterDataNodes(scaleUpTimer.nodes(), zoneDescriptor));
        }

        if (sdtt > cdnt && sdtt <= timestampLong) {
            dataNodes.removeAll(scaleDownTimer.nodes());

            if (sdtt > sutt) {
                newTimestamp = scaleDownTimer.timeToTrigger();
            }
        }

        return pair(newTimestamp, dataNodes);
    }

    public CompletableFuture<Set<String>> dataNodes(int zoneId, HybridTimestamp timestamp) {
        return getValueFromMetaStorage(zoneDataNodesHistoryKey(zoneId), DataNodesHistorySerializer::deserialize)
                .thenApply(history -> {
                    Pair<HybridTimestamp, Set<NodeWithAttributes>> entry = history.dataNodesForTimestamp(timestamp);

                    return entry.getSecond().stream().map(NodeWithAttributes::nodeName).collect(toSet());
                });
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

    static Operation addNewEntryToDataNodesHistory(
            int zoneId,
            DataNodesHistory history,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> nodes
    ) {
        if (!history.isEmpty() && nodes.equals(history.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).getSecond())) {
            return noop();
        } else {
            return put(zoneDataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(history.addHistoryEntry(timestamp, nodes)));
        }
    }

    private static Operation renewTimer(ByteArray timerKey, DistributionZoneTimer timer) {
        return put(timerKey, DistributionZoneTimerSerializer.serialize(timer));
    }

    private static Operation renewTimerOrClearIfAlreadyApplied(ByteArray timerKey, HybridTimestamp timestamp, DistributionZoneTimer timer) {
        DistributionZoneTimer newValue = timer.timeToTrigger().longValue() > timestamp.longValue() ? timer : DEFAULT_TIMER;

        return renewTimer(timerKey, newValue);
    }

    static Operation clearTimer(ByteArray timerKey) {
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

        return CompletableFuture.completedFuture(dataNodeHistoryContextFromValues(entries));
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
                    String keyString = new String(e.key(), StandardCharsets.UTF_8);
                    assert keyString.startsWith(DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX) : "Unexpected key: " + keyString;

                    int zoneId = extractZoneId(e.key(), DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX.getBytes(StandardCharsets.UTF_8));

                    DistributionZoneTimer timer = DistributionZoneTimerSerializer.deserialize(e.value());

                    CatalogZoneDescriptor zoneDescriptor = catalogManager.activeCatalog(event.timestamp().longValue()).zone(zoneId);

                    assert zoneDescriptor != null : "Zone descriptor must be present [zoneId=" + zoneId + "].";

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
                    String keyString = new String(e.key(), StandardCharsets.UTF_8);
                    assert keyString.startsWith(DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX) : "Unexpected key: " + keyString;

                    int zoneId = extractZoneId(e.key(), DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX.getBytes(StandardCharsets.UTF_8));

                    DistributionZoneTimer timer = DistributionZoneTimerSerializer.deserialize(e.value());

                    CatalogZoneDescriptor zoneDescriptor = catalogManager.activeCatalog(event.timestamp().longValue()).zone(zoneId);

                    assert zoneDescriptor != null : "Zone descriptor must be present [zoneId=" + zoneId + "].";

                    onScaleDownTimerChange(zoneDescriptor, timer);
                }
            }

            return nullCompletedFuture();
        });
    }

    @TestOnly
    public ZoneTimers zoneTimers(int zoneId) {
        return zoneTimers.get(zoneId);
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

                delay = 0;
            }
        }

        synchronized void stopTimer() {
            if (taskFuture != null) {
                taskFuture.cancel(false);
            }
        }

        @TestOnly
        synchronized boolean taskIsScheduled() {
            return taskFuture != null;
        }

        @TestOnly
        synchronized boolean taskIsCancelled() {
            assert taskFuture != null;
            return taskFuture.isCancelled();
        }

        @TestOnly
        synchronized boolean taskIsDone() {
            assert taskFuture != null;
            return taskFuture.isDone();
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
