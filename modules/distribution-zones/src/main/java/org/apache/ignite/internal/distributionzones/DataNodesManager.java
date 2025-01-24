package org.apache.ignite.internal.distributionzones;

import static java.lang.Math.max;
import static java.util.Collections.emptySet;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonePartitionResetTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerKey;
import static org.apache.ignite.internal.lang.Pair.pair;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.Pair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
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

    private static final DistributionZoneTimer DEFAULT_TIMER = new DistributionZoneTimer(HybridTimestamp.MIN_VALUE, Set.of());

    private final MetaStorageManager metaStorageManager;

    /**
     * Map with zone id as a key and set of zone timers as a value.
     */
    private final Map<Integer, ZoneTimers> zoneTimers  = new ConcurrentHashMap<>();

    /** Executor for scheduling tasks for scale up and scale down processes. */
    private final StripedScheduledThreadPoolExecutor executor;

    public DataNodesManager(String nodeName, MetaStorageManager metaStorageManager) {
        this.metaStorageManager = metaStorageManager;

        executor = createZoneManagerExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
                NamedThreadFactory.create(nodeName, "dst-zones-scheduler", LOG)
        );
    }

    void start(
            Collection<CatalogZoneDescriptor> knownZones,
            Set<NodeWithAttributes> logicalTopology
    ) {
        for (CatalogZoneDescriptor zone : knownZones) {
            onScaleUpTimerChange(zone, logicalTopology);
            onScaleDownTimerChange(zone, logicalTopology);
        }
    }

    void stop() {
        zoneTimers.forEach((k, zt) -> zt.stopAllTimers());

        shutdownAndAwaitTermination(executor, 10, SECONDS);
    }

    public CompletableFuture<Void> onTopologyChangeZoneHandler(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> oldLogicalTopology,
            Set<NodeWithAttributes> newLogicalTopology
    ) {
        return msInvokeWithRetry(() -> {
            int zoneId = zoneDescriptor.id();

            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

            if (dataNodesHistory.history.containsKey(timestamp)) {
                return null;
            }

            DistributionZoneTimer scaleUpTimer = scaleUpTimer(zoneId);
            DistributionZoneTimer scaleDownTimer = scaleDownTimer(zoneId);

            Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                    timestamp,
                    dataNodesHistory,
                    oldLogicalTopology,
                    scaleUpTimer,
                    scaleDownTimer,
                    zoneDescriptor
            );

            Set<NodeWithAttributes> filteredNewTopology = filterDataNodes(newLogicalTopology, zoneDescriptor);

            Set<NodeWithAttributes> addedNodes = filteredNewTopology.stream()
                    .filter(node -> !currentDataNodes.getSecond().contains(node))
                    .collect(toSet());

            Set<NodeWithAttributes> removedNodes = currentDataNodes.getSecond().stream()
                    .filter(node -> !newLogicalTopology.contains(node))
                    .collect(toSet());

            if ((!addedNodes.isEmpty() || !removedNodes.isEmpty()) && zoneDescriptor.dataNodesAutoAdjust() != INFINITE_TIMER_VALUE) {
                // TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
                throw new UnsupportedOperationException("Data nodes auto adjust is not supported.");
            }

            // TODO deal with partition distribution reset

            DistributionZoneTimer newScaleUpTimer = new DistributionZoneTimer(
                    timestamp.addPhysicalTime(zoneDescriptor.dataNodesAutoAdjustScaleUp() * 1000L),
                    union(addedNodes, scaleUpTimer.timestamp.longValue() < timestamp.longValue() ? emptySet() : scaleUpTimer.nodes)
            );

            DistributionZoneTimer newScaleDownTimer = new DistributionZoneTimer(
                    timestamp.addPhysicalTime(zoneDescriptor.dataNodesAutoAdjustScaleDown() * 1000L),
                    union(removedNodes, scaleDownTimer.timestamp.longValue() < timestamp.longValue() ? emptySet() : scaleDownTimer.nodes)
            );

            return iif(
                    and(
                            dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                            and(
                                    timerEqualToOrNotExists(zoneScaleUpTimerKey(zoneId), scaleUpTimer),
                                    timerEqualToOrNotExists(zoneScaleDownTimerKey(zoneId), scaleDownTimer)
                            )
                    ),
                    ops(
                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, timestamp, currentDataNodes.getSecond()),
                            renewTimer(zoneScaleUpTimerKey(zoneId), newScaleUpTimer),
                            renewTimer(zoneScaleDownTimerKey(zoneId), newScaleDownTimer)
                    ).yield(true),
                    ops().yield(false)
            );
        });
    }

    public CompletableFuture<Void> onZoneFilterChange(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> logicalTopology
    ) {
        return msInvokeWithRetry(() -> {
            int zoneId = zoneDescriptor.id();

            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

            if (dataNodesHistory.history.containsKey(timestamp)) {
                return null;
            }

            Set<NodeWithAttributes> dataNodes = filterDataNodes(logicalTopology, zoneDescriptor);

            return iif(
                    dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                    ops(
                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, timestamp, dataNodes),
                            clearTimer(zoneScaleUpTimerKey(zoneId)),
                            clearTimer(zoneScaleDownTimerKey(zoneId)),
                            clearTimer(zonePartitionResetTimerKey(zoneId))
                    ).yield(true),
                    ops().yield(false)
            );
        });
    }

    public CompletableFuture<Void> onAutoAdjustAlteration(
            CatalogZoneDescriptor zoneDescriptor,
            HybridTimestamp timestamp,
            int oldAutoAdjustScaleUp,
            int oldAutoAdjustScaleDown,
            Set<NodeWithAttributes> logicalTopology
    ) {
        return msInvokeWithRetry(() -> {
            int zoneId = zoneDescriptor.id();

            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

            if (dataNodesHistory.history.containsKey(timestamp)) {
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
                    logicalTopology,
                    newScaleUpTimer,
                    newScaleDownTimer,
                    zoneDescriptor
            );

            return iif(
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
            );
        });
    }

    public void onUpdatePartitionDistributionReset(
            int zoneId,
            int partitionDistributionResetTimeoutSeconds,
            Runnable task
    ) {
        if (partitionDistributionResetTimeoutSeconds == INFINITE_TIMER_VALUE) {
            zoneTimers.computeIfAbsent(zoneId, ZoneTimers::new).partitionReset.stopScheduledTask();
        } else {
            zoneTimers.computeIfAbsent(zoneId, ZoneTimers::new).partitionReset.reschedule(partitionDistributionResetTimeoutSeconds, task);
        }
    }

    private CompletableFuture<Void> onScaleUpTimerChange(
            CatalogZoneDescriptor zoneDescriptor,
            Set<NodeWithAttributes> logicalTopology
    ) {
        int zoneId = zoneDescriptor.id();

        DistributionZoneTimer scaleUpTimer = scaleUpTimer(zoneId);

        Runnable runnable = () -> msInvokeWithRetry(() -> {
            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

            if (dataNodesHistory.history.containsKey(scaleUpTimer.timestamp)) {
                return null;
            }

            DistributionZoneTimer scaleUpTimer0 = scaleUpTimer(zoneId);

            Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                    scaleUpTimer.timestamp,
                    dataNodesHistory,
                    logicalTopology,
                    scaleUpTimer,
                    DEFAULT_TIMER,
                    zoneDescriptor
            );

            return iif(
                    and(
                            dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                            timerEqualToOrNotExists(zoneScaleUpTimerKey(zoneId), scaleUpTimer0)
                    ),
                    ops(
                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, scaleUpTimer.timestamp, currentDataNodes.getSecond()),
                            clearTimer(zoneScaleUpTimerKey(zoneId))
                    ).yield(true),
                    ops().yield(false)
            );
        });

        rescheduleScaleUp(delayInSeconds(scaleUpTimer.timestamp), runnable, zoneId);

        return completedFuture(null);
    }

    private void onScaleDownTimerChange(
            CatalogZoneDescriptor zoneDescriptor,
            Set<NodeWithAttributes> logicalTopology
    ) {
        int zoneId = zoneDescriptor.id();

        DistributionZoneTimer scaleDownTimer = scaleDownTimer(zoneId);

        Runnable runnable = () -> msInvokeWithRetry(() -> {
            DataNodesHistory dataNodesHistory = dataNodesHistory(zoneId);

            if (dataNodesHistory.history.containsKey(scaleDownTimer.timestamp)) {
                return null;
            }

            DistributionZoneTimer scaleDownTimer0 = scaleDownTimer(zoneId);

            Pair<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodes = currentDataNodes(
                    scaleDownTimer.timestamp,
                    dataNodesHistory,
                    logicalTopology,
                    DEFAULT_TIMER,
                    scaleDownTimer,
                    zoneDescriptor
            );

            return iif(
                    and(
                            dataNodesHistoryEqualToOrNotExists(zoneId, dataNodesHistory),
                            timerEqualToOrNotExists(zoneScaleDownTimerKey(zoneId), scaleDownTimer0)
                    ),
                    ops(
                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, scaleDownTimer.timestamp, currentDataNodes.getSecond()),
                            clearTimer(zoneScaleDownTimerKey(zoneId))
                    ).yield(true),
                    ops().yield(false)
            );
        });

        rescheduleScaleDown(delayInSeconds(scaleDownTimer.timestamp), runnable, zoneId);
    }

    private void onPartitionResetTimerChange(
            CatalogZoneDescriptor zoneDescriptor,
            Set<NodeWithAttributes> logicalTopology
    ) {
        // TODO
    }

    private long delayInSeconds(HybridTimestamp timerTimestamp) {
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
            @Nullable DataNodesHistory dataNodesHistory,
            Set<NodeWithAttributes> oldLogicalTopology,
            DistributionZoneTimer scaleUpTimer,
            DistributionZoneTimer scaleDownTimer,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        Map.Entry<HybridTimestamp, Set<NodeWithAttributes>> currentDataNodesEntry = dataNodesHistory.history.floorEntry(timestamp);

        Set<NodeWithAttributes> currentDataNodes;
        HybridTimestamp currentDataNodesTimestamp;

        if (currentDataNodesEntry == null) {
            currentDataNodesTimestamp = HybridTimestamp.MIN_VALUE;
            currentDataNodes = filterDataNodes(oldLogicalTopology, zoneDescriptor);
        } else {
            currentDataNodesTimestamp = currentDataNodesEntry.getKey();
            currentDataNodes = currentDataNodesEntry.getValue();
        }

        long cdnt = currentDataNodesEntry == null ? HybridTimestamp.MIN_VALUE.longValue() : currentDataNodesEntry.getKey().longValue();
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
                    Map.Entry<HybridTimestamp, Set<NodeWithAttributes>> entry = history.history.floorEntry(timestamp);

                    if (entry == null) {
                        return emptySet();
                    }

                    return entry.getValue().stream().map(NodeWithAttributes::nodeName).collect(toSet());
                });
    }

    private DataNodesHistory dataNodesHistory(int zoneId) {
        return ofNullable(getValueFromMetaStorageLocally(zoneDataNodesHistoryKey(zoneId), DataNodesHistorySerializer::deserialize))
                .orElse(new DataNodesHistory(new TreeMap<>()));
    }

    private DistributionZoneTimer scaleUpTimer(int zoneId) {
        return ofNullable(getValueFromMetaStorageLocally(zoneScaleUpTimerKey(zoneId), DistributionZoneTimerSerializer::deserialize))
                .orElse(DEFAULT_TIMER);
    }

    private DistributionZoneTimer scaleDownTimer(int zoneId) {
        return ofNullable(getValueFromMetaStorageLocally(zoneScaleDownTimerKey(zoneId), DistributionZoneTimerSerializer::deserialize))
                .orElse(DEFAULT_TIMER);
    }

    private Condition dataNodesHistoryEqualToOrNotExists(int zoneId, DataNodesHistory history) {
        return or(
                notExists(zoneDataNodesHistoryKey(zoneId)),
                value(zoneDataNodesHistoryKey(zoneId)).eq(DataNodesHistorySerializer.serialize(history))
        );
    }

    private Condition timerEqualToOrNotExists(ByteArray timerKey, DistributionZoneTimer timer) {
        return or(
                notExists(timerKey),
                or(
                    value(timerKey).eq(DistributionZoneTimerSerializer.serialize(timer)),
                    value(timerKey).eq(DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER))
                )
        );
    }

    public static Operation addNewEntryToDataNodesHistory(
            int zoneId,
            DataNodesHistory history,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> nodes
    ) {
        DataNodesHistory newHistory = new DataNodesHistory(new TreeMap<>(history.history));
        newHistory.history.put(timestamp, nodes);

        return put(zoneDataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(newHistory));
    }

    private static Operation renewTimer(ByteArray timerKey, DistributionZoneTimer timer) {
        return put(timerKey, DistributionZoneTimerSerializer.serialize(timer));
    }

    private static Operation renewTimerOrClearIfAlreadyApplied(ByteArray timerKey, HybridTimestamp timestamp, DistributionZoneTimer timer) {
        DistributionZoneTimer newValue = timer.timestamp.longValue() > timestamp.longValue() ? timer : DEFAULT_TIMER;

        return renewTimer(timerKey, newValue);
    }

    public static Operation clearTimer(ByteArray timerKey) {
        return put(timerKey, DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER));
    }

    @Nullable
    private <T> T getValueFromMetaStorageLocally(ByteArray key, Function<byte[], T> deserializer) {
        return deserializeEntry(metaStorageManager.getLocally(key), deserializer);
    }

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

    private CompletableFuture<Void> msInvokeWithRetry(Supplier<Iif> iifSupplier) {
        Iif iif = iifSupplier.get();

        if (iif == null) {
            return completedFuture(null);
        }

        return metaStorageManager.invoke(iif)
                .handle((v, e) -> {
                    if (e == null) {
                        if (v.getAsBoolean()) {
                            return null;
                        } else {
                            return msInvokeWithRetry(iifSupplier);
                        }
                    } else {
                        LOG.warn("Failed to perform meta storage invoke", e);

                        return null;
                    }
                })
                .thenCompose(Function.identity());
    }

    void onZoneDrop(int zoneId, HybridTimestamp timestamp) {
        ZoneTimers zt = zoneTimers.remove(zoneId);
        if (zt != null) {
            zt.stopAllTimers();
        }
    }

    public static class DataNodesHistory {
        final NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history;

        public DataNodesHistory() {
            this(new TreeMap<>());
        }

        private DataNodesHistory(NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history) {
            this.history = history;
        }
    }

    private static class DataNodesHistorySerializer extends VersionedSerializer<DataNodesHistory> {
        private static final DataNodesHistorySerializer INSTANCE = new DataNodesHistorySerializer();

        @Override
        protected void writeExternalData(DataNodesHistory object, IgniteDataOutput out) throws IOException {
            out.writeMap(
                    object.history,
                    (k, out0) -> out0.writeLong(k.longValue()),
                    (v, out0) -> out0.writeCollection(v, NodeWithAttributesSerializer.INSTANCE::writeExternal)
            );
        }

        @Override
        protected DataNodesHistory readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
            NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history = in.readMap(
                    TreeMap::new,
                    in0 -> HybridTimestamp.hybridTimestamp(in0.readLong()),
                    in0 -> in0.readCollection(
                            HashSet::new,
                            NodeWithAttributesSerializer.INSTANCE::readExternal
                    )
            );

            return new DataNodesHistory(history);
        }

        static byte[] serialize(DataNodesHistory dataNodesHistory) {
            return VersionedSerialization.toBytes(dataNodesHistory, INSTANCE);
        }

        static DataNodesHistory deserialize(byte[] bytes) {
            return VersionedSerialization.fromBytes(bytes, INSTANCE);
        }
    }

    private static class DistributionZoneTimer {
        final HybridTimestamp timestamp;

        final Set<NodeWithAttributes> nodes;

        private DistributionZoneTimer(HybridTimestamp timestamp, Set<NodeWithAttributes> nodes) {
            this.timestamp = timestamp;
            this.nodes = nodes;
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

            taskFuture = executor.schedule(task, delayInSeconds, SECONDS);
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
}
