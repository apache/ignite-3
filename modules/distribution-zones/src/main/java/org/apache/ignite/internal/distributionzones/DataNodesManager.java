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
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.HIGH_AVAILABILITY;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
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
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
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
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Manager for data nodes of distribution zones.
 */
public class DataNodesManager {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DataNodesManager.class);

    private static final int MAX_ATTEMPTS_ON_RETRY = 100;

    private static final DistributionZoneTimer DEFAULT_TIMER = new DistributionZoneTimer(HybridTimestamp.MIN_VALUE, Set.of());

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

            return getDataNodeValues(zoneId, msGetter).thenApply(dataNodeValues -> {
                DataNodesHistory dataNodesHistory = dataNodeValues.dataNodesHistory;

                if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
                    return null;
                }

                LOG.info("Topology change detected [zoneId={}, timestamp={}, newTopology={}].", zoneId, timestamp,
                        nodeNames(newLogicalTopology));

                DistributionZoneTimer scaleUpTimer = dataNodeValues.scaleUpTimer;
                DistributionZoneTimer scaleDownTimer = dataNodeValues.scaleDownTimer;

                Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                        timestamp,
                        dataNodesHistory,
                        scaleUpTimer,
                        scaleDownTimer,
                        zoneDescriptor
                );

                Set<NodeWithAttributes> filteredNewTopology = filterDataNodes(newLogicalTopology, zoneDescriptor);

                Set<NodeWithAttributes> addedNodes = filteredNewTopology.stream()
                        .filter(node -> !currentDataNodes.getSecond().contains(node))
                        .collect(toSet());

                Set<NodeWithAttributes> removedNodes = currentDataNodes.getSecond().stream()
                        .filter(node -> !newLogicalTopology.contains(node) && !Objects.equals(node.nodeName(), localNodeName))
                        .collect(toSet());

                if ((!addedNodes.isEmpty() || !removedNodes.isEmpty()) && zoneDescriptor.dataNodesAutoAdjust() != INFINITE_TIMER_VALUE) {
                    // TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
                    throw new UnsupportedOperationException("Data nodes auto adjust is not supported.");
                }

                if (!removedNodes.isEmpty() && zoneDescriptor.consistencyMode() == HIGH_AVAILABILITY) {
                    if (partitionResetDelay != INFINITE_TIMER_VALUE) {
                        reschedulePartitionReset(partitionResetDelay, taskOnPartitionReset, zoneId);
                    }
                }

                DistributionZoneTimer newScaleUpTimer = newScaleUpTimerOnTopologyChange(zoneDescriptor, timestamp, scaleUpTimer, addedNodes);

                DistributionZoneTimer newScaleDownTimer = newScaleDownTimerOnTopologyChange(zoneDescriptor, timestamp, scaleDownTimer,
                        removedNodes);

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
                        "Updated data nodes on topology change, added history entry [zoneId=" + zoneId + ", timestamp=" + timestamp + ", nodes="
                                + nodeNames(currentDataNodes.getSecond()) + "], scaleUpTimer=" + newScaleUpTimer
                                + ", scaleDownTimer=" + newScaleDownTimer + "].",
                        "Failed to update data nodes history and timers on topology change [timestamp=" + timestamp + "]."
                );
            });
        });
    }

    private DistributionZoneTimer newScaleUpTimerOnTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DistributionZoneTimer currentTimer,
            Set<NodeWithAttributes> addedNodes
    ) {
        if (addedNodes.isEmpty() || zoneDescriptor.dataNodesAutoAdjustScaleUp() == INFINITE_TIMER_VALUE) {
            return DEFAULT_TIMER;
        } else {
            return new DistributionZoneTimer(
                    zoneDescriptor.dataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE
                            ? timestamp.tick()
                            : timestamp.addPhysicalTime(zoneDescriptor.dataNodesAutoAdjustScaleUp() * 1000L),
                    union(addedNodes, currentTimer.timestamp.longValue() < timestamp.longValue() ? emptySet() : currentTimer.nodes)
            );
        }
    }

    private DistributionZoneTimer newScaleDownTimerOnTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DistributionZoneTimer currentTimer,
            Set<NodeWithAttributes> removedNodes
    ) {
        if (removedNodes.isEmpty() || zoneDescriptor.dataNodesAutoAdjustScaleDown() == INFINITE_TIMER_VALUE) {
            return DEFAULT_TIMER;
        } else {
            return new DistributionZoneTimer(
                    zoneDescriptor.dataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE
                            ? timestamp.tick()
                            : timestamp.addPhysicalTime(zoneDescriptor.dataNodesAutoAdjustScaleDown() * 1000L),
                    union(removedNodes, currentTimer.timestamp.longValue() < timestamp.longValue() ? emptySet() : currentTimer.nodes)
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

            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

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
                "Updated data nodes on distribution zone filter change, added history entry, timers are cleared [zoneId=" + zoneId
                        + ", timestamp=" + timestamp + ", nodes=" + nodeNames(dataNodes) + "].",
                "Failed to update data nodes history and timers on distribution zone filter change [timestamp=" + timestamp + "]."
            );
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

            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

            if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
                return null;
            }

            DistributionZoneTimer scaleUpTimer = scaleUpTimer(zoneId);
            DistributionZoneTimer scaleDownTimer = scaleDownTimer(zoneId);

            DistributionZoneTimer newScaleUpTimer = new DistributionZoneTimer(
                    timestamp
                            .subtractPhysicalTime(oldAutoAdjustScaleUp * 1000L)
                            .addPhysicalTime(zoneDescriptor.dataNodesAutoAdjustScaleUp() * 1000L),
                    scaleUpTimer.nodes
            );

            DistributionZoneTimer newScaleDownTimer = new DistributionZoneTimer(
                    timestamp
                            .subtractPhysicalTime(oldAutoAdjustScaleDown * 1000L)
                            .addPhysicalTime(zoneDescriptor.dataNodesAutoAdjustScaleDown() * 1000L),
                    scaleUpTimer.nodes
            );

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
                "Updated data nodes on distribution zone auto adjust change, added history entry [zoneId=" + zoneId
                        + ", timestamp=" + timestamp + ", nodes=" + nodeNames(currentDataNodes.getSecond())
                        + "], scaleUpTimer=" + newScaleUpTimer + ", scaleDownTimer=" + newScaleDownTimer + "].",
                "Failed to update data nodes history and timers on distribution zone auto adjust change [timestamp=" + timestamp + "]."
            );
        });
    }

    void onUpdatePartitionDistributionReset(
            int zoneId,
            int partitionDistributionResetTimeoutSeconds,
            Runnable taskOnReset
    ) {
        if (partitionDistributionResetTimeoutSeconds == INFINITE_TIMER_VALUE) {
            zoneTimers.computeIfAbsent(zoneId, ZoneTimers::new).partitionReset.stopScheduledTask();
        } else {
            zoneTimers.computeIfAbsent(zoneId, ZoneTimers::new).partitionReset
                    .reschedule(partitionDistributionResetTimeoutSeconds, taskOnReset);
        }
    }

    private void onScaleUpTimerChange(CatalogZoneDescriptor zoneDescriptor) {
        int zoneId = zoneDescriptor.id();

        DistributionZoneTimer scaleUpTimer = scaleUpTimer(zoneId);

        if (scaleUpTimer.equals(DEFAULT_TIMER)) {
            return;
        }

        onScaleUpTimerChange(zoneDescriptor, scaleUpTimer);
    }

    private void onScaleUpTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleUpTimer) {
        if (scaleUpTimer.equals(DEFAULT_TIMER)) {
            return;
        }

        int zoneId = zoneDescriptor.id();

        Runnable runnable = () -> msInvokeWithRetry(msGetter -> {
            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

            DistributionZoneTimer scaleUpTimer0 = scaleUpTimer(zoneId);

            LOG.info("Scale up timer triggered [zoneId={}, timer={}].", zoneId, scaleUpTimer0);

            if (dataNodesHistory.entryIsPresentAtExactTimestamp(scaleUpTimer0.timestamp)) {
                return null;
            }

            Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                    scaleUpTimer0.timestamp,
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
                                addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, scaleUpTimer0.timestamp, currentDataNodes.getSecond()),
                                clearTimer(zoneScaleUpTimerKey(zoneId))
                        ).yield(true),
                        ops().yield(false)
                ),
                "Updated data nodes on scale up timer trigger, added history entry, scale up timer is cleared [zoneId=" + zoneId
                        + ", timestamp=" + scaleUpTimer0.timestamp + ", nodes=" + nodeNames(currentDataNodes.getSecond()) + "].",
                "Failed to update data nodes history and timers on scale up timer trigger [timestamp=" + scaleUpTimer0.timestamp + "]."
            );
        });

        rescheduleScaleUp(delayInSeconds(scaleUpTimer.timestamp), runnable, zoneId);
    }

    private void onScaleDownTimerChange(CatalogZoneDescriptor zoneDescriptor) {
        int zoneId = zoneDescriptor.id();

        DistributionZoneTimer scaleDownTimer = scaleDownTimer(zoneId);

        if (scaleDownTimer.equals(DEFAULT_TIMER)) {
            return;
        }

        onScaleDownTimerChange(zoneDescriptor, scaleDownTimer);
    }

    private void onScaleDownTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleDownTimer) {
        if (scaleDownTimer.equals(DEFAULT_TIMER)) {
            return;
        }

        int zoneId = zoneDescriptor.id();

        Runnable runnable = () -> msInvokeWithRetry(msGetter -> {
            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

            DistributionZoneTimer scaleDownTimer0 = scaleDownTimer(zoneId);

            LOG.info("Scale down timer triggered [zoneId={}, timer={}].", zoneId, scaleDownTimer0);

            if (dataNodesHistory.entryIsPresentAtExactTimestamp(scaleDownTimer0.timestamp)) {
                return null;
            }

            Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                    scaleDownTimer0.timestamp,
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
                                        scaleDownTimer0.timestamp, currentDataNodes.getSecond()),
                                clearTimer(zoneScaleDownTimerKey(zoneId))
                        ).yield(true),
                        ops().yield(false)
                ),
                "Updated data nodes on scale down timer trigger, added history entry, scale down timer is cleared [zoneId=" + zoneId
                        + ", timestamp=" + scaleDownTimer0.timestamp + ", nodes=" + nodeNames(currentDataNodes.getSecond()) + "].",
                "Failed to update data nodes history and timers on scale down timer trigger [timestamp=" + scaleDownTimer0.timestamp + "]."
            );
        });

        rescheduleScaleDown(delayInSeconds(scaleDownTimer.timestamp), runnable, zoneId);
    }

    private static long delayInSeconds(HybridTimestamp timerTimestamp) {
        long currentTime = System.currentTimeMillis();

        long delayMs = timerTimestamp.getPhysical() - currentTime;

        return max(0, delayMs / 1000);
    }

    private void rescheduleScaleUp(long delayInSeconds, Runnable runnable, int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, ZoneTimers::new).scaleUp.reschedule(delayInSeconds, runnable);
    }

    private void rescheduleScaleDown(long delayInSeconds, Runnable runnable, int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, ZoneTimers::new).scaleDown.reschedule(delayInSeconds, runnable);
    }

    private void reschedulePartitionReset(long delayInSeconds, Runnable runnable, int zoneId) {
        zoneTimers.computeIfAbsent(zoneId, ZoneTimers::new).partitionReset.reschedule(delayInSeconds, runnable);
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

        Set<NodeWithAttributes> currentDataNodes;
        HybridTimestamp currentDataNodesTimestamp;

        currentDataNodesTimestamp = currentDataNodesEntry.getFirst();
        currentDataNodes = currentDataNodesEntry.getSecond();

        long cdnt = currentDataNodesEntry.getFirst().longValue();
        long sutt = scaleUpTimer.timestamp.longValue();
        long sdtt = scaleDownTimer.timestamp.longValue();
        long timestampLong = timestamp.longValue();

        if (sutt > cdnt && sutt <= timestampLong) {
            currentDataNodesTimestamp = scaleUpTimer.timestamp;
            currentDataNodes.addAll(filterDataNodes(scaleUpTimer.nodes, zoneDescriptor));
        }

        if (sdtt > cdnt && sdtt > sutt && sdtt <= timestampLong) {
            currentDataNodesTimestamp = scaleDownTimer.timestamp;
            currentDataNodes.removeAll(scaleDownTimer.nodes);
        }

        return pair(currentDataNodesTimestamp, currentDataNodes);
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
        return put(zoneDataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(history.addHistoryEntry(timestamp, nodes)));
    }

    private static Operation renewTimer(ByteArray timerKey, DistributionZoneTimer timer) {
        return put(timerKey, DistributionZoneTimerSerializer.serialize(timer));
    }

    private static Operation renewTimerOrClearIfAlreadyApplied(ByteArray timerKey, HybridTimestamp timestamp, DistributionZoneTimer timer) {
        DistributionZoneTimer newValue = timer.timestamp.longValue() > timestamp.longValue() ? timer : DEFAULT_TIMER;

        return renewTimer(timerKey, newValue);
    }

    static Operation clearTimer(ByteArray timerKey) {
        return put(timerKey, DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER));
    }

    private CompletableFuture<DataNodeValues> getDataNodeValues(int zoneId, MSGetter msGetter) {
        return msGetter.getValuesFromMetaStorage(Set.of(
                        zoneDataNodesHistoryKey(zoneId),
                        zoneScaleUpTimerKey(zoneId),
                        zoneScaleDownTimerKey(zoneId)
                ))
                .thenApply(entries -> {
                    for (Entry e : entries) {
                        assert e != null && e.value() != null && !e.empty() && !e.tombstone() : "Unexpected entry: " + e;
                    }

                    return new DataNodeValues(
                            DataNodesHistorySerializer.deserialize(entries.get(0).value()),
                            DistributionZoneTimerSerializer.deserialize(entries.get(1).value()),
                            DistributionZoneTimerSerializer.deserialize(entries.get(2).value())
                    );
                });
    }

    @Nullable
    private <T> CompletableFuture<T> getValueFromMetaStorage(ByteArray key, Function<byte[], T> deserializer) {
        return metaStorageManager.get(key).thenApply(e -> deserializeEntry(e, deserializer));
    }

    @Nullable
    private static <T> T deserializeEntry(@Nullable Entry e, Function<byte[], T> deserializer) {
        if (e == null || e.value() == null || e.empty() || e.tombstone()) {
            return null;
        } else {
            return deserializer.apply(e.value());
        }
    }

    private CompletableFuture<Void> msInvokeWithRetry(Function<MSGetter, CompletableFuture<LoggedIif>> iifSupplier) {
        return msInvokeWithRetry(iifSupplier, MAX_ATTEMPTS_ON_RETRY);
    }

    private CompletableFuture<Void> msInvokeWithRetry(Function<MSGetter, CompletableFuture<LoggedIif>> iifSupplier, int attemptsLeft) {
        if (attemptsLeft <= 0) {
            LOG.error("Failed to perform meta storage invoke, maximum number of attempts reached.");

            return nullCompletedFuture();
        }

        CompletableFuture<LoggedIif> iifFuture = iifSupplier.apply(attemptsLeft == MAX_ATTEMPTS_ON_RETRY
                ? this::getValueFromMetaStorageLocally
                : this::getValueFromMetaStorage
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

                    CatalogZoneDescriptor zoneDescriptor = catalogManager.zone(zoneId, event.timestamp().longValue());

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

                    CatalogZoneDescriptor zoneDescriptor = catalogManager.zone(zoneId, event.timestamp().longValue());

                    assert zoneDescriptor != null : "Zone descriptor must be present [zoneId=" + zoneId + "].";

                    onScaleDownTimerChange(zoneDescriptor, timer);
                }
            }

            return nullCompletedFuture();
        });
    }

    private static class DistributionZoneTimer {
        final HybridTimestamp timestamp;

        final Set<NodeWithAttributes> nodes;

        private DistributionZoneTimer(HybridTimestamp timestamp, Set<NodeWithAttributes> nodes) {
            this.timestamp = timestamp;
            this.nodes = nodes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DistributionZoneTimer that = (DistributionZoneTimer) o;
            return Objects.equals(timestamp, that.timestamp) && Objects.equals(nodes, that.nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, nodes);
        }

        @Override
        public String toString() {
            return "[timestamp=" + timestamp + ", nodes=" + nodeNames(nodes) + ']';
        }
    }

    private static class DistributionZoneTimerSerializer extends VersionedSerializer<DistributionZoneTimer> {
        /** Serializer instance. */
        private static final DistributionZoneTimerSerializer INSTANCE = new DistributionZoneTimerSerializer();

        @Override
        protected void writeExternalData(DistributionZoneTimer object, IgniteDataOutput out) throws IOException {
            out.writeLong(object.timestamp.longValue());
            out.writeCollection(object.nodes, NodeWithAttributesSerializer.INSTANCE::writeExternal);
        }

        @Override
        protected DistributionZoneTimer readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
            HybridTimestamp timestamp = HybridTimestamp.hybridTimestamp(in.readLong());
            Set<NodeWithAttributes> nodes = in.readCollection(HashSet::new, NodeWithAttributesSerializer.INSTANCE::readExternal);

            return new DistributionZoneTimer(timestamp, nodes);
        }

        static byte[] serialize(DistributionZoneTimer timer) {
            return VersionedSerialization.toBytes(timer, INSTANCE);
        }

        static DistributionZoneTimer deserialize(byte[] bytes) {
            return VersionedSerialization.fromBytes(bytes, INSTANCE);
        }
    }

    private class ZoneTimerSchedule {
        final int zoneId;
        ScheduledFuture<?> taskFuture;
        long delay;

        ZoneTimerSchedule(int zoneId) {
            this.zoneId = zoneId;
        }

        synchronized void reschedule(long delayInSeconds, Runnable task) {
            stopScheduledTask();

            taskFuture = executor.schedule(task, delayInSeconds, SECONDS, zoneId);
            delay = delayInSeconds;
        }

        synchronized void stopScheduledTask() {
            if (taskFuture != null && delay > 0) {
                taskFuture.cancel(false);

                delay = 0;
            }
        }
    }

    private class ZoneTimers {
        final ZoneTimerSchedule scaleUp;
        final ZoneTimerSchedule scaleDown;
        final ZoneTimerSchedule partitionReset;

        ZoneTimers(int zoneId) {
            this.scaleUp = new ZoneTimerSchedule(zoneId);
            this.scaleDown = new ZoneTimerSchedule(zoneId);
            this.partitionReset = new ZoneTimerSchedule(zoneId);
        }

        void stopAllTimers() {
            scaleUp.stopScheduledTask();
            scaleDown.stopScheduledTask();
            partitionReset.stopScheduledTask();
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

    private interface MSGetter {
        @Nullable
        CompletableFuture<List<Entry>> getValuesFromMetaStorage(Set<ByteArray> keys);
    }

    private static class DataNodeValues {
        final DataNodesHistory dataNodesHistory;
        final DistributionZoneTimer scaleUpTimer;
        final DistributionZoneTimer scaleDownTimer;

        DataNodeValues(DataNodesHistory dataNodesHistory, DistributionZoneTimer scaleUpTimer, DistributionZoneTimer scaleDownTimer) {
            this.dataNodesHistory = dataNodesHistory;
            this.scaleUpTimer = scaleUpTimer;
            this.scaleDownTimer = scaleDownTimer;
        }
    }
}
