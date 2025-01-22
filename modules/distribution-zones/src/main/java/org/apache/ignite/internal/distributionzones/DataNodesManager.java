package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.emptySet;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.lang.Pair.pair;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.CollectionUtils.union;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
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
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

public class DataNodesManager {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DataNodesManager.class);

    private static final DistributionZoneTimer DEFAULT_TIMER = new DistributionZoneTimer(HybridTimestamp.MIN_VALUE, Set.of());

    private final MetaStorageManager metaStorageManager;

    /**
     * Map with states for distribution zones. States are needed to track nodes that we want to add or remove from the data nodes,
     * schedule and stop scale up and scale down processes.
     */
    private final Map<Integer, ZoneState> zonesState = new ConcurrentHashMap<>();

    public DataNodesManager(MetaStorageManager metaStorageManager) {
        this.metaStorageManager = metaStorageManager;
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
                                    timerEqualToOrNotExists(scaleUpTimerKey(zoneId), scaleUpTimer),
                                    timerEqualToOrNotExists(scaleDownTimerKey(zoneId), scaleDownTimer)
                            )
                    ),
                    ops(
                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, timestamp, currentDataNodes.getSecond()),
                            renewTimer(scaleUpTimerKey(zoneId), newScaleUpTimer),
                            renewTimer(scaleDownTimerKey(zoneId), newScaleDownTimer)
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
                            clearTimer(scaleUpTimerKey(zoneId)),
                            clearTimer(scaleDownTimerKey(zoneId)),
                            clearTimer(resetTimerKey(zoneId))
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
                                    timerEqualToOrNotExists(scaleUpTimerKey(zoneId), scaleUpTimer),
                                    timerEqualToOrNotExists(scaleDownTimerKey(zoneId), scaleDownTimer)
                            )
                    ),
                    ops(
                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, timestamp, currentDataNodes.getSecond()),
                            renewTimerOrClearIfAlreadyApplied(scaleUpTimerKey(zoneId), timestamp, newScaleUpTimer),
                            renewTimerOrClearIfAlreadyApplied(scaleDownTimerKey(zoneId), timestamp, newScaleDownTimer)
                    ).yield(true),
                    ops().yield(false)
            );
        });
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
                            timerEqualToOrNotExists(scaleUpTimerKey(zoneId), scaleUpTimer0)
                    ),
                    ops(
                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, scaleUpTimer.timestamp, currentDataNodes.getSecond()),
                            clearTimer(scaleUpTimerKey(zoneId))
                    ).yield(true),
                    ops().yield(false)
            );
        });

        rescheduleScaleUp(zoneDescriptor.dataNodesAutoAdjustScaleUp(), runnable, zoneId);

        return completedFuture(null);
    }

    private CompletableFuture<Void> onScaleDownTimerChange(
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
                            timerEqualToOrNotExists(scaleDownTimerKey(zoneId), scaleDownTimer0)
                    ),
                    ops(
                            addNewEntryToDataNodesHistory(zoneId, dataNodesHistory, scaleDownTimer.timestamp, currentDataNodes.getSecond()),
                            clearTimer(scaleDownTimerKey(zoneId))
                    ).yield(true),
                    ops().yield(false)
            );
        });

        rescheduleScaleDown(zoneDescriptor.dataNodesAutoAdjustScaleDown(), runnable, zoneId);

        return completedFuture(null);
    }

    public synchronized void rescheduleScaleUp(long delay, Runnable runnable, int zoneId) {
        // TODO schedule the task
    }

    public synchronized void rescheduleScaleDown(long delay, Runnable runnable, int zoneId) {
        // TODO schedule the task
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
        return getValueFromMetaStorage(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer::deserialize)
                .thenApply(history -> {
                    Map.Entry<HybridTimestamp, Set<NodeWithAttributes>> entry = history.history.floorEntry(timestamp);

                    if (entry == null) {
                        return emptySet();
                    }

                    return entry.getValue().stream().map(NodeWithAttributes::nodeName).collect(toSet());
                });
    }

    private DataNodesHistory dataNodesHistory(int zoneId) {
        return ofNullable(getValueFromMetaStorageLocally(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer::deserialize))
                .orElse(new DataNodesHistory(new TreeMap<>()));
    }

    private DistributionZoneTimer scaleUpTimer(int zoneId) {
        return ofNullable(getValueFromMetaStorageLocally(scaleUpTimerKey(zoneId), DistributionZoneTimerSerializer::deserialize))
                .orElse(DEFAULT_TIMER);
    }

    private DistributionZoneTimer scaleDownTimer(int zoneId) {
        return ofNullable(getValueFromMetaStorageLocally(scaleDownTimerKey(zoneId), DistributionZoneTimerSerializer::deserialize))
                .orElse(DEFAULT_TIMER);
    }

    private Condition dataNodesHistoryEqualToOrNotExists(int zoneId, DataNodesHistory history) {
        return or(
                notExists(dataNodesHistoryKey(zoneId)),
                value(dataNodesHistoryKey(zoneId)).eq(DataNodesHistorySerializer.serialize(history))
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

    private Operation addNewEntryToDataNodesHistory(
            int zoneId,
            DataNodesHistory history,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> nodes
    ) {
        DataNodesHistory newHistory = new DataNodesHistory(new TreeMap<>(history.history));
        newHistory.history.put(timestamp, nodes);

        return put(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(newHistory));
    }

    private Operation renewTimer(ByteArray timerKey, DistributionZoneTimer timer) {
        return put(timerKey, DistributionZoneTimerSerializer.serialize(timer));
    }

    private Operation renewTimerOrClearIfAlreadyApplied(ByteArray timerKey, HybridTimestamp timestamp, DistributionZoneTimer timer) {
        DistributionZoneTimer newValue = timer.timestamp.longValue() > timestamp.longValue() ? timer : DEFAULT_TIMER;

        return renewTimer(timerKey, newValue);
    }

    private Operation clearTimer(ByteArray timerKey) {
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

    private ByteArray dataNodesHistoryKey(int zoneId) {
        return new ByteArray("dataNodesHistory_" + zoneId);
    }

    private ByteArray scaleUpTimerKey(int zoneId) {
        return new ByteArray("scaleUpTimer_" + zoneId);
    }

    private ByteArray scaleDownTimerKey(int zoneId) {
        return new ByteArray("scaleDownTimer_" + zoneId);
    }

    private ByteArray resetTimerKey(int zoneId) {
        return new ByteArray("resetTimer_" + zoneId);
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

    private static class DataNodesHistory {
        final NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history;

        private DataNodesHistory(NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history) {
            this.history = history;
        }
    }

    private static class DataNodesHistorySerializer extends VersionedSerializer<DataNodesHistory> {
        /** Serializer instance. */
        public static final DataNodesHistorySerializer INSTANCE = new DataNodesHistorySerializer();

        @Override
        protected void writeExternalData(DataNodesHistory object, IgniteDataOutput out) throws IOException {

        }

        @Override
        protected DataNodesHistory readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
            return null;
        }

        public static byte[] serialize(DataNodesHistory dataNodesHistory) {
            return VersionedSerialization.toBytes(dataNodesHistory, INSTANCE);
        }

        public static DataNodesHistory deserialize(byte[] bytes) {
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
        public static final DistributionZoneTimerSerializer INSTANCE = new DistributionZoneTimerSerializer();

        @Override
        protected void writeExternalData(DistributionZoneTimer object, IgniteDataOutput out) throws IOException {

        }

        @Override
        protected DistributionZoneTimer readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
            return null;
        }

        public static byte[] serialize(DistributionZoneTimer timer) {
            return VersionedSerialization.toBytes(timer, INSTANCE);
        }

        public static DistributionZoneTimer deserialize(byte[] bytes) {
            return VersionedSerialization.fromBytes(bytes, INSTANCE);
        }
    }
}
