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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodeHistoryContextFromValues;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deserializeLogicalTopologySet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deserializeNodesAttributes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.nodeNames;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributes;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractZoneId;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notTombstone;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.PartitionResetClosure;
import org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DistributionZoneTimerSerializer;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DataNodesHistoryContext;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
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
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.Lazy;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Manager for data nodes of distribution zones.
 * <br>
 * It is used by {@link DistributionZoneManager} to calculate data nodes, see {@link #dataNodes(int, HybridTimestamp)}.
 * Data nodes are stored in meta storage as {@link DataNodesHistory} for each zone, also for each zone there are scale up and
 * scale down timers, stored as {@link DistributionZoneTimer}. Partition reset timer is calculated on the node recovery according
 * to the scale down timer state, see {@link #restorePartitionResetTimer(CatalogZoneDescriptor, DistributionZoneTimer, long)}.
 * <br>
 * Data nodes history is appended on topology changes, on zone filter changes and on zone auto adjust alterations (i.e. alterations of
 * scale up and scale down timer configuration), see {@link #onTopologyChange}, {@link #onZoneFilterChange} and
 * {@link #onAutoAdjustAlteration} methods respectively.
 */
public class DataNodesManager {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DataNodesManager.class);

    private static final int MAX_ATTEMPTS_ON_RETRY = 100;

    private final MetaStorageManager metaStorageManager;

    private final CatalogManager catalogManager;

    private final ClockService clockService;

    private final FailureProcessor failureProcessor;

    /** External busy lock. */
    private final IgniteSpinBusyLock busyLock;

    /**
     * Map with zone id as a key and set of zone timers as a value.
     */
    private final Map<Integer, ZoneTimers> zoneTimers  = new ConcurrentHashMap<>();

    /** Executor for scheduling tasks for scale up and scale down processes. */
    private final StripedScheduledThreadPoolExecutor executor;

    private final Lazy<UUID> localNodeId;

    private final WatchListener scaleUpTimerPrefixListener;

    private final WatchListener scaleDownTimerPrefixListener;

    private final WatchListener dataNodesListener;

    private final Map<Integer, DataNodesHistory> dataNodesHistoryVolatile = new ConcurrentHashMap<>();

    private final PartitionResetClosure partitionResetClosure;

    private final IntSupplier partitionDistributionResetTimeoutSupplier;

    private final Supplier<Set<NodeWithAttributes>> latestLogicalTopologyProvider;

    /**
     * Constructor.
     *
     * @param nodeName Local node name.
     * @param nodeIdSupplier Node id supplier.
     * @param busyLock External busy lock.
     * @param metaStorageManager Meta storage manager.
     * @param catalogManager Catalog manager.
     * @param clockService Clock service.
     * @param failureProcessor Failure processor.
     * @param partitionResetClosure Closure to reset partitions.
     * @param partitionDistributionResetTimeoutSupplier Supplier for partition distribution reset timeout.
     * @param latestLogicalTopologyProvider Provider of the latest logical topology.
     */
    public DataNodesManager(
            String nodeName,
            Supplier<UUID> nodeIdSupplier,
            IgniteSpinBusyLock busyLock,
            MetaStorageManager metaStorageManager,
            CatalogManager catalogManager,
            ClockService clockService,
            FailureProcessor failureProcessor,
            PartitionResetClosure partitionResetClosure,
            IntSupplier partitionDistributionResetTimeoutSupplier,
            Supplier<Set<NodeWithAttributes>> latestLogicalTopologyProvider
    ) {
        this.metaStorageManager = metaStorageManager;
        this.catalogManager = catalogManager;
        this.clockService = clockService;
        this.failureProcessor = failureProcessor;
        this.localNodeId = new Lazy<>(nodeIdSupplier);
        this.partitionResetClosure = partitionResetClosure;
        this.partitionDistributionResetTimeoutSupplier = partitionDistributionResetTimeoutSupplier;
        this.latestLogicalTopologyProvider = latestLogicalTopologyProvider;
        this.busyLock = busyLock;

        executor = createZoneManagerExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
                IgniteThreadFactory.create(nodeName, "dst-zones-scheduler", LOG)
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
            allKeys.add(zoneDataNodesHistoryKey(zone.id()));
            allKeys.add(zoneScaleUpTimerKey(zone.id()));
            allKeys.add(zoneScaleDownTimerKey(zone.id()));
            allKeys.add(zoneDataNodesKey(zone.id()));

            descriptors.put(zone.id(), zone);
        }

        List<CompletableFuture<?>> legacyInitFutures = new ArrayList<>();

        return metaStorageManager.getAll(allKeys)
                .thenAccept(entriesMap -> {
                    for (CatalogZoneDescriptor zone : descriptors.values()) {
                        Entry historyEntry = entriesMap.get(zoneDataNodesHistoryKey(zone.id()));
                        Entry scaleUpEntry = entriesMap.get(zoneScaleUpTimerKey(zone.id()));
                        Entry scaleDownEntry = entriesMap.get(zoneScaleDownTimerKey(zone.id()));
                        Entry legacyDataNodesEntry = entriesMap.get(zoneDataNodesKey(zone.id()));

                        if (missingEntry(historyEntry)) {
                            if (missingEntry(legacyDataNodesEntry)) {
                                // Not critical because if we have no history in this map, we look into meta storage.
                                LOG.warn("Couldn't recover data nodes history for zone [id={}, historyEntry={}].", zone.id(), historyEntry);
                            } else {
                                legacyInitFutures.add(initZoneWithLegacyDataNodes(
                                        zone,
                                        legacyDataNodesEntry.value(),
                                        recoveryRevision
                                ));
                            }

                            continue;
                        }

                        if (missingEntry(scaleUpEntry) || missingEntry(scaleDownEntry)) {
                            throw new AssertionError(format("Couldn't recover timers for zone [id={}, name={}, scaleUpEntry={}, "
                                    + "scaleDownEntry={}", zone.id(), zone.name(), scaleUpEntry, scaleDownEntry));
                        }

                        DataNodesHistory history = DataNodesHistorySerializer.deserialize(historyEntry.value());
                        dataNodesHistoryVolatile.put(zone.id(), history);

                        DistributionZoneTimer scaleUpTimer = DistributionZoneTimerSerializer.deserialize(scaleUpEntry.value());
                        DistributionZoneTimer scaleDownTimer = DistributionZoneTimerSerializer.deserialize(scaleDownEntry.value());

                        onScaleUpTimerChange(zone, scaleUpTimer);
                        onScaleDownTimerChange(zone, scaleDownTimer);
                        // We can restore partition reset timer based on scale down timer, so we don't need to persist it.
                        restorePartitionResetTimer(zone, scaleDownTimer, recoveryRevision);
                    }
                })
                .thenCompose(unused -> allOf(legacyInitFutures)
                        .handle((v, e) -> {
                            if (e != null) {
                                LOG.warn("Could not recover legacy data nodes for zone.", e);
                            }

                            return nullCompletedFuture();
                        })
                )
                .thenAccept(unused -> { /* No-op. */ });
    }

    private static boolean missingEntry(Entry e) {
        return e.empty() || e.tombstone();
    }

    void stop() {
        zoneTimers.forEach((k, zt) -> zt.stopAllTimers());

        shutdownAndAwaitTermination(executor, 10, SECONDS);
    }

    /**
     * Recalculates data nodes on topology changes, modifies scale up and scale down timers and appends data nodes history in meta storage.
     * It takes the current data nodes according to timestamp, compares them with the new topology and calculates added nodes and
     * removed nodes, also compares with the old topology and calculates added nodes comparing to the old topology (if they are not empty
     * then the history entry will be always added).
     * <br>
     * Added nodes and removed nodes are added to scale up and scale down timers respectively. Their time to trigger is refreshed
     * according to current zone descriptor. If their time to trigger is less than or equal to  the current timestamp (including the cases
     * when auto adjust timeout is immediate), then they are applied automatically to the new history entry, in this cases their value
     * in meta storage is set to {@link DistributionZoneTimer#DEFAULT_TIMER}.
     * <br>
     * For example:
     * <ul>
     *     <li>there are nodes A, B in the latest data nodes history entry, its timestamp is 1;</li>
     *     <li>scale up auto adjust is 5, scale down auto adjust is 100 (let's say that time units for auto adjust are the same as used
     *     for timestamps);</li>
     *     <li>node A leaves, node C joins at timestamp 10;</li>
     *     <li>onTopologyChange is triggered and sets scale up timer to 15 and scale down to 110;</li>
     *     <li>for some reason scheduled executor hangs and doesn't trigger timers (let's consider case when everything is recalculated in
     *     onTopologyChange);</li>
     *     <li>node B leaves at time 20;</li>
     *     <li>onTopologyChange is triggered again and sees that scale up timer should have been already triggered (its time to trigger
     *     was 15), it adds node C automatically to new history entry. Scale down timer is scheduled and has to wait more, but
     *     another node (B) left, so scale down timer's nodes are now A and B, and its time to trigger is rescheduled and is now
     *     20 + 100 = 120. Data nodes in the new data nodes history entry are A, B, C.</li>
     * </ul>
     *
     * @param zoneDescriptor Zone descriptor.
     * @param revision Meta storage revision.
     * @param timestamp Timestamp, that is consistent with meta storage revision.
     * @param newLogicalTopology New logical topology.
     * @param oldLogicalTopology Old logical topology.
     * @return CompletableFuture that is completed when the operation is done.
     */
    CompletableFuture<Void> onTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            long revision,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> newLogicalTopology,
            Set<NodeWithAttributes> oldLogicalTopology
    ) {
        int zoneId = zoneDescriptor.id();

        return doOperation(
                zoneDescriptor,
                List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleUpTimerKey(zoneId), zoneScaleDownTimerKey(zoneId)),
                dataNodesHistoryContext -> completedFuture(onTopologyChangeInternal(
                        zoneDescriptor,
                        revision,
                        timestamp,
                        newLogicalTopology,
                        oldLogicalTopology,
                        dataNodesHistoryContext
                )),
                false
        );
    }

    @Nullable
    private DataNodesHistoryMetaStorageOperation onTopologyChangeInternal(
            CatalogZoneDescriptor zoneDescriptor,
            long revision,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> newLogicalTopology,
            Set<NodeWithAttributes> oldLogicalTopology,
            @Nullable DistributionZonesUtil.DataNodesHistoryContext dataNodesHistoryContext
    ) {
        if (dataNodesHistoryContext == null) {
            // This means that the zone was not initialized yet. The initial history entry with current topology will
            // be written on the zone init.
            return null;
        }

        DataNodesHistory dataNodesHistory = dataNodesHistoryContext.dataNodesHistory();

        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
            // This event was already processed by another node.
            return null;
        }

        int zoneId = zoneDescriptor.id();

        LOG.debug("Topology change detected [zoneId={}, timestamp={}, newTopology={}, oldTopology={}].", zoneId, timestamp,
                nodeNames(newLogicalTopology), nodeNames(oldLogicalTopology));

        DistributionZoneTimer scaleUpTimer = dataNodesHistoryContext.scaleUpTimer();
        DistributionZoneTimer scaleDownTimer = dataNodesHistoryContext.scaleDownTimer();

        DataNodesHistoryEntry latestDataNodes = dataNodesHistory.dataNodesForTimestamp(timestamp);

        Set<NodeWithAttributes> addedNodes = newLogicalTopology.stream()
                .filter(node -> !latestDataNodes.dataNodes().contains(node))
                .collect(toSet());

        Set<NodeWithAttributes> addedNodesComparingToOldTopology = newLogicalTopology.stream()
                .filter(node -> !oldLogicalTopology.contains(node))
                .collect(toSet());

        Set<NodeWithAttributes> removedNodes = latestDataNodes.dataNodes().stream()
                .filter(node -> !newLogicalTopology.contains(node) && !Objects.equals(node.nodeId(), localNodeId.get()))
                .filter(node -> !scaleDownTimer.nodes().contains(node))
                .collect(toSet());

        int partitionResetDelay = partitionDistributionResetTimeoutSupplier.getAsInt();

        if (!removedNodes.isEmpty()
                && zoneDescriptor.consistencyMode() == HIGH_AVAILABILITY
                && partitionResetDelay != INFINITE_TIMER_VALUE) {
            reschedulePartitionReset(partitionResetDelay, () -> partitionResetClosure.run(revision, zoneDescriptor), zoneId);
        }

        DistributionZoneTimer mergedScaleUpTimer = mergeTimerOnTopologyChange(zoneDescriptor, timestamp,
                scaleUpTimer, addedNodes, newLogicalTopology, true);

        DistributionZoneTimer mergedScaleDownTimer = mergeTimerOnTopologyChange(zoneDescriptor, timestamp,
                scaleDownTimer, removedNodes, newLogicalTopology, false);

        DataNodesHistoryEntry currentDataNodes = currentDataNodes(
                timestamp,
                dataNodesHistory,
                mergedScaleUpTimer,
                mergedScaleDownTimer,
                zoneDescriptor
        );

        DistributionZoneTimer scaleUpTimerToSave = timerToSave(timestamp, mergedScaleUpTimer);
        DistributionZoneTimer scaleDownTimerToSave = timerToSave(timestamp, mergedScaleDownTimer);

        boolean addMandatoryEntry = !addedNodesComparingToOldTopology.isEmpty();

        Condition condition = and(
                dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                and(
                        timerEqualToOrNotExists(zoneScaleUpTimerKey(zoneId), scaleUpTimer),
                        timerEqualToOrNotExists(zoneScaleDownTimerKey(zoneId), scaleDownTimer)
                )
        );

        List<Operation> operations = operations(
                addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, currentDataNodes.timestamp(),
                        currentDataNodes.dataNodes(), addMandatoryEntry),
                renewTimer(zoneScaleUpTimerKey(zoneId), scaleUpTimerToSave),
                renewTimer(zoneScaleDownTimerKey(zoneId), scaleDownTimerToSave)
        );

        return DataNodesHistoryMetaStorageOperation.builder()
                .zoneId(zoneId)
                .condition(condition)
                .operations(operations)
                .operationName("topology change")
                .currentDataNodesHistory(dataNodesHistory)
                .currentTimestamp(timestamp)
                .historyEntryTimestamp(currentDataNodes.timestamp())
                .historyEntryNodes(currentDataNodes.dataNodes())
                .scaleUpTimer(scaleUpTimerToSave)
                .scaleDownTimer(scaleDownTimerToSave)
                .addMandatoryEntry(addMandatoryEntry)
                .build();
    }

    /**
     * This is called on topology change, when some nodes may be added or removed, and therefore scale up or scale down timers can be
     * created. By this moment there can be already some timers in meta storage, so we need to merge them with new topology changes, that
     * is done by this method. The create time of the new timer is set to current timestamp, and sets of nodes in two timers are unified,
     * with consideration of the logical topology: if the node is absent in topology, it is excluded from scale up timer, and if it is
     * present, it is excluded from scale down timer.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param timestamp Current timestamp.
     * @param currentTimer Current timer, that is taken from meta storage.
     * @param nodes Set of nodes that are added or removed, depending on the timer, this is defined by {@code scaleUp} parameter.
     * @param logicalTopology Current logical topology.
     * @param scaleUp If true, the timer is scale up, otherwise it is scale down.
     * @return Merged timer.
     */
    private static DistributionZoneTimer mergeTimerOnTopologyChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DistributionZoneTimer currentTimer,
            Set<NodeWithAttributes> nodes,
            Set<NodeWithAttributes> logicalTopology,
            boolean scaleUp
    ) {
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
            return new DistributionZoneTimer(
                    currentTimer.createTimestamp(),
                    currentTimer.timeToWaitInSeconds(),
                    currentTimerFilteredNodes
            );
        } else {
            int autoAdjustWaitInSeconds = scaleUp
                    ? zoneDescriptor.dataNodesAutoAdjustScaleUp()
                    : zoneDescriptor.dataNodesAutoAdjustScaleDown();

            return new DistributionZoneTimer(
                    timestamp,
                    autoAdjustWaitInSeconds,
                    union(nodes, currentTimerFilteredNodes)
            );
        }
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

    /**
     * Recalculates data nodes on zone filter changes according only to current topology and new filters. Stops and discards all timers.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param timestamp Current timestamp.
     * @param logicalTopology New logical topology.
     * @return CompletableFuture that is completed when the operation is done.
     */
    CompletableFuture<Void> onZoneFilterChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> logicalTopology
    ) {
        int zoneId = zoneDescriptor.id();

        return doOperation(
                zoneDescriptor,
                List.of(zoneDataNodesHistoryKey(zoneId)),
                dataNodesHistoryContext -> completedFuture(onZoneFilterChangeInternal(
                        zoneDescriptor,
                        timestamp,
                        logicalTopology,
                        dataNodesHistoryContext
                )),
                true
        );
    }

    @Nullable
    private DataNodesHistoryMetaStorageOperation onZoneFilterChangeInternal(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> logicalTopology,
            DataNodesHistoryContext dataNodesHistoryContext
    ) {
        LOG.debug("Distribution zone filter changed [zoneId={}, timestamp={}, logicalTopology={}, descriptor={}].", zoneDescriptor.id(),
                timestamp, nodeNames(logicalTopology), zoneDescriptor);

        Set<NodeWithAttributes> filteredDataNodes = filterDataNodes(logicalTopology, zoneDescriptor);

        return recalculateAndApplyDataNodesToMetastoreImmediately(
                zoneDescriptor,
                filteredDataNodes,
                timestamp,
                dataNodesHistoryContext,
                "distribution zone filter change"
        );
    }

    /**
     * Recalculates data nodes on zone auto adjust alterations. Modifies scale up and scale down timers and appends data nodes history.
     * The new data nodes are calculated according to the new timer values. See also description of {@link #onTopologyChange} method
     * for more details and examples of how data nodes are recalculated.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param timestamp Current timestamp.
     * @return CompletableFuture that is completed when the operation is done.
     */
    CompletableFuture<Void> onAutoAdjustAlteration(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp
    ) {
        int zoneId = zoneDescriptor.id();

        return doOperation(
                zoneDescriptor,
                List.of(zoneDataNodesHistoryKey(zoneId), zoneScaleUpTimerKey(zoneId), zoneScaleDownTimerKey(zoneId)),
                dataNodesHistoryContext -> completedFuture(onAutoAdjustAlterationInternal(
                        zoneDescriptor,
                        timestamp,
                        dataNodesHistoryContext
                )),
                true
        );
    }

    @Nullable
    private DataNodesHistoryMetaStorageOperation onAutoAdjustAlterationInternal(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            DataNodesHistoryContext dataNodesHistoryContext
    ) {
        assert dataNodesHistoryContext != null : "Data nodes history and timers are missing, zone=" + zoneDescriptor;

        DataNodesHistory dataNodesHistory = dataNodesHistoryContext.dataNodesHistory();

        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
            return null;
        }

        int zoneId = zoneDescriptor.id();

        LOG.debug("Distribution zone auto adjust changed [zoneId={}, timestamp={}, descriptor={}].",
                zoneId, timestamp, zoneDescriptor);

        DistributionZoneTimer scaleUpTimer = dataNodesHistoryContext.scaleUpTimer();
        DistributionZoneTimer scaleDownTimer = dataNodesHistoryContext.scaleDownTimer();

        DistributionZoneTimer modifiedScaleUpTimer = scaleUpTimer
                .modifyTimeToWait(zoneDescriptor.dataNodesAutoAdjustScaleUp());

        DistributionZoneTimer modifiedScaleDownTimer = scaleDownTimer
                .modifyTimeToWait(zoneDescriptor.dataNodesAutoAdjustScaleDown());

        DataNodesHistoryEntry currentDataNodes = currentDataNodes(
                timestamp,
                dataNodesHistory,
                modifiedScaleUpTimer,
                modifiedScaleDownTimer,
                zoneDescriptor
        );

        DistributionZoneTimer scaleUpTimerToSave = timerToSave(timestamp, modifiedScaleUpTimer);
        DistributionZoneTimer scaleDownTimerToSave = timerToSave(timestamp, modifiedScaleDownTimer);

        Condition condition = and(
                dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                and(
                        timerEqualToOrNotExists(zoneScaleUpTimerKey(zoneId), scaleUpTimer),
                        timerEqualToOrNotExists(zoneScaleDownTimerKey(zoneId), scaleDownTimer)
                )
        );

        List<Operation> operations = operations(
                addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, currentDataNodes.timestamp(),
                        currentDataNodes.dataNodes()),
                renewTimer(zoneScaleUpTimerKey(zoneId), scaleUpTimerToSave),
                renewTimer(zoneScaleDownTimerKey(zoneId), scaleDownTimerToSave)
        );

        return DataNodesHistoryMetaStorageOperation.builder()
                .zoneId(zoneId)
                .condition(condition)
                .operations(operations)
                .operationName("distribution zone auto adjust change")
                .currentDataNodesHistory(dataNodesHistory)
                .currentTimestamp(timestamp)
                .historyEntryTimestamp(currentDataNodes.timestamp())
                .historyEntryNodes(currentDataNodes.dataNodes())
                .scaleUpTimer(scaleUpTimerToSave)
                .scaleDownTimer(scaleDownTimerToSave)
                .build();
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

    /**
     * The closure that is executed on scale up or scale down schedule. Appends data nodes history in meta storage and sets the timer
     * to default value.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param scheduledTimer Scheduled timer.
     * @return Runnable that is executed on schedule.
     */
    private Runnable applyTimerClosure(CatalogZoneDescriptor zoneDescriptor, ScheduledTimer scheduledTimer) {
        int zoneId = zoneDescriptor.id();

        return () -> doOperation(
                zoneDescriptor,
                List.of(zoneDataNodesHistoryKey(zoneId), scheduledTimer.metaStorageKey()),
                dataNodesHistoryContext -> applyTimerClosure0(zoneDescriptor, scheduledTimer, dataNodesHistoryContext),
                true
        );
    }

    @Nullable
    private CompletableFuture<DataNodesHistoryMetaStorageOperation> applyTimerClosure0(
            CatalogZoneDescriptor zoneDescriptor,
            ScheduledTimer scheduledTimer,
            DataNodesHistoryContext dataNodesHistoryContext
    ) {
        assert dataNodesHistoryContext != null : "Data nodes history and timers are missing, zone=" + zoneDescriptor;

        DataNodesHistory dataNodesHistory = dataNodesHistoryContext.dataNodesHistory();

        DistributionZoneTimer timer = scheduledTimer.timerFromContext(dataNodesHistoryContext);

        if (timer.equals(DEFAULT_TIMER)) {
            return nullCompletedFuture();
        }

        int zoneId = zoneDescriptor.id();

        LOG.debug("Triggered " + scheduledTimer.name() + " [zoneId={}, timer={}].", zoneId, timer);

        HybridTimestamp timeToTrigger = timer.timeToTrigger();

        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timeToTrigger)) {
            return nullCompletedFuture();
        }

        long currentDelay = delayInSeconds(timeToTrigger);

        if (currentDelay < 0) {
            return nullCompletedFuture();
        }

        if (currentDelay > 1) {
            scheduledTimer.reschedule(currentDelay, applyTimerClosure(zoneDescriptor, scheduledTimer));

            return nullCompletedFuture();
        }

        DataNodesHistoryEntry currentDataNodes = scheduledTimer
                .recalculateDataNodes(dataNodesHistory, timer);

        // We need to wait for actual time according to hybrid clock plus clock skew, because scheduled executor
        // is not synchronized with hybrid clock. If we don't do this and scheduled executor will trigger this closure
        // earlier than max hybrid clock time in cluster, we may append new history entry having timestamp that
        // is (actually) in future.
        return clockService.waitFor(timeToTrigger.addPhysicalTime(clockService.maxClockSkewMillis()))
                .thenApply(ignored -> DataNodesHistoryMetaStorageOperation.builder()
                        .zoneId(zoneId)
                        .condition(
                                and(
                                        dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                                        timerEqualToOrNotExists(scheduledTimer.metaStorageKey(), timer)
                                )
                        )
                        .operations(
                                operations(
                                        addNewEntryToDataNodesHistory(
                                                zoneId,
                                                dataNodesHistory,
                                                timeToTrigger,
                                                currentDataNodes.dataNodes()
                                        ),
                                        clearTimer(scheduledTimer.metaStorageKey())
                                )
                        )
                        .operationName(scheduledTimer.name() + " trigger")
                        .currentDataNodesHistory(dataNodesHistory)
                        .currentTimestamp(timeToTrigger)
                        .historyEntryTimestamp(timeToTrigger)
                        .historyEntryNodes(currentDataNodes.dataNodes())
                        .scaleUpTimer(scheduledTimer.scaleUpTimerAfterApply())
                        .scaleDownTimer(scheduledTimer.scaleDownTimerAfterApply())
                        .build()
                );
    }

    private void onScaleUpTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleUpTimer) {
        ScheduledTimer timer = new ScaleUpScheduledTimer(zoneDescriptor);
        timer.init(scaleUpTimer);
    }

    private void onScaleDownTimerChange(CatalogZoneDescriptor zoneDescriptor, DistributionZoneTimer scaleDownTimer) {
        ScheduledTimer timer = new ScaleDownScheduledTimer(zoneDescriptor);
        timer.init(scaleDownTimer);
    }

    private void restorePartitionResetTimer(CatalogZoneDescriptor zone, DistributionZoneTimer scaleDownTimer, long revision) {
        if (!scaleDownTimer.equals(DEFAULT_TIMER) && zone.consistencyMode() == HIGH_AVAILABILITY) {
            reschedulePartitionReset(
                    partitionDistributionResetTimeoutSupplier.getAsInt(),
                    () -> partitionResetClosure.run(revision, zone),
                    zone.id()
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
     * @return History entry of current timestamp and data nodes.
     */
    private static DataNodesHistoryEntry currentDataNodes(
            HybridTimestamp timestamp,
            DataNodesHistory dataNodesHistory,
            DistributionZoneTimer scaleUpTimer,
            DistributionZoneTimer scaleDownTimer,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        DataNodesHistoryEntry currentDataNodesEntry = dataNodesHistory.dataNodesForTimestamp(timestamp);

        assert currentDataNodesEntry.timestamp().longValue() != HybridTimestamp.MIN_VALUE.longValue()
                : "Data nodes history is missing for timestamp [zoneId=" + zoneDescriptor.id() + ", timestamp=" + timestamp + "].";

        Set<NodeWithAttributes> dataNodes = new HashSet<>(currentDataNodesEntry.dataNodes());

        long scaleUpTriggerTime = scaleUpTimer.timeToTrigger().longValue();
        long scaleDownTriggerTime = scaleDownTimer.timeToTrigger().longValue();
        long timestampLong = timestamp.longValue();
        HybridTimestamp newTimestamp = timestamp;

        if (scaleUpTriggerTime <= timestampLong) {
            dataNodes.addAll(filterDataNodes(scaleUpTimer.nodes(), zoneDescriptor));
        }

        if (scaleDownTriggerTime <= timestampLong) {
            dataNodes.removeAll(scaleDownTimer.nodes());
        }

        return new DataNodesHistoryEntry(newTimestamp, dataNodes);
    }

    /**
     * Returns data nodes for the given zone and timestamp. See {@link #dataNodes(int, HybridTimestamp, Integer)}.
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
     * Returns data nodes for the given zone and timestamp. It doesn't recalculate the data nodes, it just retrieves them from data nodes
     * history. For information how data nodes are calculated, see {@link #onTopologyChange}, {@link #onZoneFilterChange} and {@link
     * #onAutoAdjustAlteration} methods.
     *
     * @param zoneId Zone ID.
     * @param timestamp Timestamp.
     * @param catalogVersion Catalog version.
     * @return Data nodes.
     */
    public CompletableFuture<Set<String>> dataNodes(int zoneId, HybridTimestamp timestamp, @Nullable Integer catalogVersion) {
        DataNodesHistory volatileHistory = dataNodesHistoryVolatile.get(zoneId);
        if (volatileHistory != null && volatileHistory.entryIsPresentAtExactTimestamp(timestamp)) {
            return completedFuture(nodeNames(volatileHistory.dataNodesForTimestamp(timestamp).dataNodes()));
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

                    DataNodesHistoryEntry entry = history.dataNodesForTimestamp(timestamp);

                    return entry.dataNodes();
                }))
                .thenApply(DistributionZonesUtil::nodeNames);
    }

    /**
     * Unlike {@link #dataNodes} this method recalculates the data nodes for given zone and writes them to metastorage.
     *
     * @param zoneName Zone name.
     * @return The future with recalculated data nodes for the given zone.
     */
    public CompletableFuture<Set<String>> recalculateDataNodes(String zoneName) {
        Objects.requireNonNull(zoneName, "Zone name is required.");

        int catalogVersion = catalogManager.latestCatalogVersion();

        CatalogZoneDescriptor zoneDescriptor = catalogManager.catalog(catalogVersion).zone(zoneName);

        if (zoneDescriptor == null) {
            return failedFuture(new DistributionZoneNotFoundException(zoneName));
        }

        return recalculateDataNodes(zoneDescriptor);
    }

    private CompletableFuture<Set<String>> recalculateDataNodes(CatalogZoneDescriptor zoneDescriptor) {
        int zoneId = zoneDescriptor.id();

        Set<NodeWithAttributes> currentLogicalTopology = topologyNodes();

        Set<NodeWithAttributes> filteredDataNodes = filterDataNodes(currentLogicalTopology, zoneDescriptor);

        return doOperation(
                zoneDescriptor,
                List.of(zoneDataNodesHistoryKey(zoneId)),
                dataNodesHistoryContext -> completedFuture(recalculateAndApplyDataNodesToMetastoreImmediately(
                        zoneDescriptor,
                        filteredDataNodes,
                        clockService.now(),
                        dataNodesHistoryContext,
                        "manual data nodes recalculation"
                )),
                true
        ).thenApply(v -> nodeNames(filteredDataNodes));
    }

    private @Nullable DataNodesHistoryMetaStorageOperation recalculateAndApplyDataNodesToMetastoreImmediately(
            CatalogZoneDescriptor zoneDescriptor,
            Set<NodeWithAttributes> filteredDataNodes,
            HybridTimestamp timestamp,
            DataNodesHistoryContext dataNodesHistoryContext,
            String operationName
    ) {
        assert dataNodesHistoryContext != null : "Data nodes history and timers are missing, zone=" + zoneDescriptor;

        DataNodesHistory dataNodesHistory = dataNodesHistoryContext.dataNodesHistory();

        if (dataNodesHistory.entryIsPresentAtExactTimestamp(timestamp)) {
            return null;
        }

        int zoneId = zoneDescriptor.id();

        stopAllTimers(zoneId);

        return DataNodesHistoryMetaStorageOperation.builder()
                .zoneId(zoneId)
                .condition(dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory))
                .operations(operations(
                        addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, timestamp, filteredDataNodes),
                        clearTimer(zoneScaleUpTimerKey(zoneId)),
                        clearTimer(zoneScaleDownTimerKey(zoneId))
                ))
                .operationName(operationName)
                .currentDataNodesHistory(dataNodesHistory)
                .currentTimestamp(timestamp)
                .historyEntryTimestamp(timestamp)
                .historyEntryNodes(filteredDataNodes)
                .scaleUpTimer(DEFAULT_TIMER)
                .scaleDownTimer(DEFAULT_TIMER)
                .build();
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

    /**
     * Meta storage operation that adds new entry to data nodes history.
     * If the new entry is the same as the latest one {@code addMandatoryEntry} is {@code false}, then no new entry is added.
     *
     * @param zoneId Zone id.
     * @param history Current data nodes history.
     * @param timestamp Timestamp of the new entry.
     * @param nodes Data nodes for the new entry.
     * @param addMandatoryEntry If {@code true}, then the new entry is added even if it is the same as the latest one.
     * @return Operation that adds new entry to data nodes history.
     */
    private Operation addNewEntryToDataNodesHistory(
            int zoneId,
            DataNodesHistory history,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> nodes,
            boolean addMandatoryEntry
    ) {
        if (!addMandatoryEntry
                && !history.isEmpty()
                && nodes.equals(history.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).dataNodes())) {
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

    private CompletableFuture<DataNodesHistoryContext> getDataNodeHistoryContextMs(List<ByteArray> keys) {
        return metaStorageManager.getAll(new HashSet<>(keys))
                .thenApply(entries -> dataNodeHistoryContextFromValues(entries.values()));
    }

    private CompletableFuture<DataNodesHistoryContext> getDataNodeHistoryContextMsLocally(List<ByteArray> keys) {
        List<Entry> entries = metaStorageManager.getAllLocally(keys);

        return completedFuture(dataNodeHistoryContextFromValues(entries));
    }

    private CompletableFuture<DataNodesHistoryContext> ensureContextIsPresentAndInitZoneIfNeeded(
            @Nullable DataNodesHistoryContext context,
            List<ByteArray> keys,
            int zoneId
    ) {
        if (context == null) {
            // Probably this is a transition from older version of cluster, need to initialize zone according to the
            // current set of meta storage entries.
            return initZone(zoneId)
                    .thenCompose(ignored -> getDataNodeHistoryContextMs(keys));
        } else {
            return completedFuture(context);
        }
    }

    @Nullable
    private static <T> T deserializeEntry(@Nullable Entry e, Function<byte[], T> deserializer) {
        if (e == null || e.value() == null || e.empty() || e.tombstone()) {
            return null;
        } else {
            return deserializer.apply(e.value());
        }
    }

    /**
     * Perform a meta storage operation for changing data nodes history and timers. Uses {@link #msInvokeWithRetry} for retries.
     *
     * @param zone Zone descriptor.
     * @param keysToRead Keys to read from meta storage, to get {@link DataNodesHistoryContext}.
     * @param operation Operation.
     * @param ensureContextIsPresent If true, then ensures that {@link DataNodesHistoryContext} is not {@code null}.
     * @return Future reflecting the completion of meta storage operation.
     */
    private CompletableFuture<Void> doOperation(
            CatalogZoneDescriptor zone,
            List<ByteArray> keysToRead,
            Function<DataNodesHistoryContext, CompletableFuture<DataNodesHistoryMetaStorageOperation>> operation,
            boolean ensureContextIsPresent
    ) {
        return msInvokeWithRetry(msGetter -> msGetter.get(keysToRead).thenCompose(operation), zone, ensureContextIsPresent);
    }

    private CompletableFuture<Void> msInvokeWithRetry(
            Function<
                    DataNodeHistoryContextMetaStorageGetter,
                    CompletableFuture<DataNodesHistoryMetaStorageOperation>
            > metaStorageOperationSupplier,
            CatalogZoneDescriptor zone,
            boolean ensureContextIsPresent
    ) {
        return msInvokeWithRetry(metaStorageOperationSupplier, MAX_ATTEMPTS_ON_RETRY, zone, ensureContextIsPresent);
    }

    /**
     * Utility method for meta storage invocation.
     *
     * @param metaStorageOperationSupplier Function that returns IIF operation to use in meta storage invocation.
     *     It contains the actual logic of operation.
     * @param attemptsLeft Number of attempts left, used if meta storage invoke did not succeed due to CAS fail.
     * @param zone Zone descriptor.
     * @return Future reflecting the completion of meta storage invocation.
     */
    private CompletableFuture<Void> msInvokeWithRetry(
            Function<
                    DataNodeHistoryContextMetaStorageGetter,
                    CompletableFuture<DataNodesHistoryMetaStorageOperation>
            > metaStorageOperationSupplier,
            int attemptsLeft,
            CatalogZoneDescriptor zone,
            boolean ensureContextIsPresent
    ) {
        if (attemptsLeft <= 0) {
            throw new AssertionError("Failed to perform meta storage invoke, maximum number of attempts reached [zone=" + zone + "].");
        }

        // Get locally on the first attempt, otherwise it means that invoke has failed because of different value in meta storage,
        // so we need to retrieve the value from meta storage.
        DataNodeHistoryContextMetaStorageGetter msGetter0 = attemptsLeft == MAX_ATTEMPTS_ON_RETRY
                ? this::getDataNodeHistoryContextMsLocally
                : this::getDataNodeHistoryContextMs;

        DataNodeHistoryContextMetaStorageGetter msGetter = ensureContextIsPresent
                ? keys -> msGetter0.get(keys).thenCompose(context -> ensureContextIsPresentAndInitZoneIfNeeded(context, keys, zone.id()))
                : msGetter0;

        CompletableFuture<DataNodesHistoryMetaStorageOperation> metaStorageOperationFuture =
                metaStorageOperationSupplier.apply(msGetter);

        return metaStorageOperationFuture
                .thenCompose(metaStorageOperation -> {
                    if (metaStorageOperation == null) {
                        return nullCompletedFuture();
                    } else {
                        // TODO https://issues.apache.org/jira/browse/IGNITE-24611
                        return metaStorageManager.invoke(metaStorageOperation.operation())
                                .thenCompose(result -> {
                                    if (result.getAsBoolean()) {
                                        LOG.info(metaStorageOperation.successLogMessage());

                                        return nullCompletedFuture();
                                    } else {
                                        return msInvokeWithRetry(
                                                metaStorageOperationSupplier,
                                                attemptsLeft - 1,
                                                zone,
                                                ensureContextIsPresent
                                        );
                                    }
                                })
                                .whenComplete((v, e) -> {
                                    if (e != null && !relatesToNodeStopping(e)) {
                                        failureProcessor.process(new FailureContext(e, metaStorageOperation.failureLogMessage()));
                                    }
                                });
                    }
                });
    }

    private static boolean relatesToNodeStopping(Throwable e) {
        return hasCause(e, NodeStoppingException.class);
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
    CompletableFuture<?> onZoneCreate(
            int zoneId,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> dataNodes
    ) {
        return initZone(zoneId, timestamp, dataNodes, false);
    }

    private CompletableFuture<?> initZoneWithLegacyDataNodes(
            CatalogZoneDescriptor zone,
            byte[] legacyDataNodesBytes,
            long recoveryRevision
    ) {
        Entry nodeAttributesEntry = metaStorageManager.getLocally(zonesNodesAttributes(), recoveryRevision);
        Map<UUID, NodeWithAttributes> nodesAttributes = deserializeNodesAttributes(nodeAttributesEntry.value());

        Set<NodeWithAttributes> unfilteredDataNodes = DataNodesMapSerializer.deserialize(legacyDataNodesBytes).keySet().stream()
                .map(node -> {
                    NodeWithAttributes nwa = nodesAttributes.get(node.nodeId());

                    if (nwa == null) {
                        return new NodeWithAttributes(node.nodeName(), node.nodeId(), null);
                    } else {
                        Map<String, String> userAttributes = nwa.userAttributes();
                        List<String> storageProfiles = nwa.storageProfiles();
                        return new NodeWithAttributes(node.nodeName(), node.nodeId(), userAttributes, storageProfiles);
                    }
                })
                .collect(toSet());

        Set<NodeWithAttributes> dataNodes = filterDataNodes(unfilteredDataNodes, zone);

        LOG.info("Recovering data nodes of distribution zone from legacy data nodes [zoneId={}, unfilteredDataNodes={}, "
                + "filter='{}', dataNodes={}]", zone.id(), nodeNames(unfilteredDataNodes), zone.filter(), nodeNames(dataNodes));

        return initZone(zone.id(), clockService.current(), dataNodes, true);
    }

    private CompletableFuture<?> initZone(int zoneId) {
        CatalogZoneDescriptor zone = zoneDescriptor(zoneId);
        Set<NodeWithAttributes> topologyNodes = latestLogicalTopologyProvider.get();
        Set<NodeWithAttributes> filteredNodes = filterDataNodes(topologyNodes, zone);

        return initZone(zoneId, clockService.now(), filteredNodes, true);
    }

    private CompletableFuture<?> initZone(
            int zoneId,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> dataNodes,
            boolean removeLegacyDataNodes
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

            Update update = new Operations(operations(
                    addNewEntryToDataNodesHistory(zoneId, new DataNodesHistory(), timestamp, dataNodes),
                    clearTimer(zoneScaleUpTimerKey(zoneId)),
                    clearTimer(zoneScaleDownTimerKey(zoneId)),
                    removeLegacyDataNodes ? remove(zoneDataNodesKey(zoneId)) : noop(),
                    removeLegacyDataNodes ? remove(zonesNodesAttributes()) : noop()
            )).yield(true);

            Iif iif = iif(condition, update, ops().yield(false));

            return metaStorageManager.invoke(iif)
                    .thenApply(StatementResult::getAsBoolean)
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            if (!relatesToNodeStopping(e)) {
                                String errorMessage = String.format(
                                        "Failed to initialize zone's dataNodes history [zoneId = %s, timestamp = %s, dataNodes = %s]",
                                        zoneId,
                                        timestamp,
                                        nodeNames(dataNodes)
                                );
                                failureProcessor.process(new FailureContext(e, errorMessage));
                            }
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
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    CompletableFuture<?> onZoneDrop(int zoneId, HybridTimestamp timestamp) {
        return removeDataNodesKeys(zoneId, timestamp)
                .thenRun(() -> {
                    ZoneTimers zt = zoneTimers.remove(zoneId);
                    if (zt != null) {
                        zt.stopAllTimers();
                    }
                });
    }

    /**
     * Method deletes data nodes related values for the specified zone.
     *
     * @param zoneId Unique id of a zone.
     * @param timestamp Timestamp of an event that has triggered this method.
     */
    private CompletableFuture<?> removeDataNodesKeys(int zoneId, HybridTimestamp timestamp) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            Condition condition = exists(zoneDataNodesHistoryKey(zoneId));

            Update removeKeysUpd = ops(
                    // TODO remove(zoneDataNodesHistoryKey(zoneId)), https://issues.apache.org/jira/browse/IGNITE-24345
                    remove(zoneScaleUpTimerKey(zoneId)),
                    remove(zoneScaleDownTimerKey(zoneId))
            ).yield(true);

            Iif iif = iif(condition, removeKeysUpd, ops().yield(false));

            return metaStorageManager.invoke(iif)
                    .thenApply(StatementResult::getAsBoolean)
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            if (!relatesToNodeStopping(e)) {
                                String errorMessage = String.format(
                                        "Failed to delete zone's dataNodes keys [zoneId = %s, timestamp = %s]",
                                        zoneId,
                                        timestamp
                                );
                                failureProcessor.process(new FailureContext(e, errorMessage));
                            }
                        } else if (invokeResult) {
                            LOG.info("Delete zone's dataNodes keys [zoneId = {}, timestamp = {}]", zoneId, timestamp);
                        } else {
                            LOG.debug("Failed to delete zone's dataNodes keys [zoneId = {}, timestamp = {}]", zoneId, timestamp);
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void stopAllTimers(int zoneId) {
        ZoneTimers zt = zoneTimers.get(zoneId);
        if (zt != null) {
            zt.stopAllTimers();
        }
    }

    private WatchListener createScaleUpTimerPrefixListener() {
        return event -> inBusyLockAsync(
                busyLock,
                () -> processWatchEvent(
                        event,
                        DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX_BYTES,
                        (zoneId, e) -> processTimerWatchEvent(zoneId, e, true)
                )
        );
    }

    private WatchListener createScaleDownTimerPrefixListener() {
        return event -> inBusyLockAsync(
                busyLock,
                () -> processWatchEvent(
                        event,
                        DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX_BYTES,
                        (zoneId, e) -> processTimerWatchEvent(zoneId, e, false)
                )
        );
    }

    private WatchListener createDataNodesListener() {
        return event -> inBusyLockAsync(
                busyLock,
                () -> processWatchEvent(event, DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES, this::processDataNodesHistoryWatchEvent)
        );
    }

    private CompletableFuture<Void> processTimerWatchEvent(int zoneId, Entry e, boolean scaleUp) {
        if (e.tombstone()) {
            return nullCompletedFuture();
        }

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

    private static CompletableFuture<Void> processWatchEvent(
            WatchEvent event,
            byte[] keyPrefix,
            BiFunction<Integer, Entry, CompletableFuture<Void>> processor
    ) {
        EntryEvent entryEvent = event.entryEvent();

        Entry e = entryEvent.newEntry();

        if (e != null && !e.empty()) {
            assert startsWith(e.key(), keyPrefix) : "Unexpected key: " + new String(e.key(), UTF_8);

            int zoneId = extractZoneId(e.key(), keyPrefix);

            return processor.apply(zoneId, e);
        }

        return nullCompletedFuture();
    }

    private CatalogZoneDescriptor zoneDescriptor(int zoneId) {
        CatalogZoneDescriptor zone = catalogManager.catalog(catalogManager.latestCatalogVersion()).zone(zoneId);

        if (zone == null) {
            throw new DistributionZoneNotFoundException(zoneId);
        }

        return zone;
    }

    /**
     * Utility method that creates list of operations filtering out NO_OP operations.
     *
     * @param operations Operations.
     * @return Operations list.
     */
    private static List<Operation> operations(Operation... operations) {
        List<Operation> res = new ArrayList<>();

        for (Operation op : operations) {
            if (op.type() != OperationType.NO_OP) {
                res.add(op);
            }
        }

        return res;
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

    @FunctionalInterface
    private interface DataNodeHistoryContextMetaStorageGetter {
        /** The future may contain {@code null} result. */
        CompletableFuture<DataNodesHistoryContext> get(List<ByteArray> keys);
    }

    /**
     * This class is used to generalize the logic of distribution zone timers related to their scheduled closure, see
     * {@link #applyTimerClosure}. The only difference in implementations is the name of timer that is used for logging,
     * the meta storage key, fields of {@link ZoneTimers} used for scheduling, etc.
     */
    private interface ScheduledTimer {
        /**
         * Name of the timer, is used for logging.
         */
        String name();

        void init(DistributionZoneTimer timer);

        ByteArray metaStorageKey();

        DistributionZoneTimer timerFromContext(DataNodesHistoryContext context);

        void reschedule(long delayInSeconds, Runnable runnable);

        DataNodesHistoryEntry recalculateDataNodes(DataNodesHistory dataNodesHistory, DistributionZoneTimer timer);

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
        public DistributionZoneTimer timerFromContext(DataNodesHistoryContext context) {
            return context.scaleUpTimer();
        }

        @Override
        public void reschedule(long delayInSeconds, Runnable runnable) {
            zoneTimers.computeIfAbsent(zone.id(), k -> createZoneTimers(zone.id())).scaleUp.reschedule(delayInSeconds, runnable);
        }

        @Override
        public DataNodesHistoryEntry recalculateDataNodes(
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
        public DistributionZoneTimer timerFromContext(DataNodesHistoryContext context) {
            return context.scaleDownTimer();
        }

        @Override
        public void reschedule(long delayInSeconds, Runnable runnable) {
            zoneTimers.computeIfAbsent(zone.id(), k -> createZoneTimers(zone.id())).scaleDown.reschedule(delayInSeconds, runnable);
        }

        @Override
        public DataNodesHistoryEntry recalculateDataNodes(
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
