package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.emptySet;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.lang.Pair.pair;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
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
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.Pair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

public class DataNodesManager implements LogicalTopologyEventListener {
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

    @Override
    public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {

    }

    @Override
    public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {

    }

    @Override
    public void onTopologyLeap(LogicalTopologySnapshot newTopology) {

    }

    private CompletableFuture<Void> onTopologyChangeZoneHandler(
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

            DistributionZoneTimer newScaleUpTimer = new DistributionZoneTimer(
                    timestamp.addPhysicalTime(zoneDescriptor.dataNodesAutoAdjustScaleUp() * 1000L),
                    union(addedNodes, scaleUpTimer.timestamp.longValue() < timestamp.longValue() ? emptySet() : scaleUpTimer.nodes)
            );

            DistributionZoneTimer newScaleDownTimer = new DistributionZoneTimer(
                    timestamp.addPhysicalTime(zoneDescriptor.dataNodesAutoAdjustScaleDown() * 1000L),
                    union(removedNodes, scaleDownTimer.timestamp.longValue() < timestamp.longValue() ? emptySet() : scaleDownTimer.nodes)
            );

            // TODO make new value for put
            dataNodesHistory.history.put(timestamp, currentDataNodes.getSecond());

            return iif(
                    and(
                            value(dataNodesHistoryKey(zoneId)).eq(DataNodesHistorySerializer.serialize(dataNodesHistory)),
                            and(
                                    value(scaleUpTimerKey(zoneId)).eq(DistributionZoneTimerSerializer.serialize(scaleUpTimer)),
                                    value(scaleDownTimerKey(zoneId)).eq(DistributionZoneTimerSerializer.serialize(scaleDownTimer))
                            )
                    ),
                    ops(
                            put(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(dataNodesHistory)),
                            put(scaleUpTimerKey(zoneId), DistributionZoneTimerSerializer.serialize(newScaleUpTimer)),
                            put(scaleDownTimerKey(zoneId), DistributionZoneTimerSerializer.serialize(newScaleDownTimer))
                    ).yield(true),
                    ops().yield(false)
            );
        });
    }

    private CompletableFuture<Void> onZoneFilterChange(
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

            DataNodesHistory newHistory = new DataNodesHistory(new TreeMap<>(dataNodesHistory.history));

            newHistory.history.put(timestamp, dataNodes);

            return iif(
                    or(
                            notExists(dataNodesHistoryKey(zoneId)),
                            value(dataNodesHistoryKey(zoneId)).eq(DataNodesHistorySerializer.serialize(dataNodesHistory))
                    ),
                    ops(
                            put(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(dataNodesHistory)),
                            put(scaleUpTimerKey(zoneId), DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER)),
                            put(scaleDownTimerKey(zoneId), DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER))
                    ).yield(true),
                    ops().yield(false)
            );
        });
    }

    private CompletableFuture<Void> onAutoAdjustAlteration(
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
                            value(dataNodesHistoryKey(zoneId)).eq(DataNodesHistorySerializer.serialize(dataNodesHistory)),
                            and(
                                    value(scaleUpTimerKey(zoneId)).eq(DistributionZoneTimerSerializer.serialize(scaleUpTimer)),
                                    value(scaleDownTimerKey(zoneId)).eq(DistributionZoneTimerSerializer.serialize(scaleDownTimer))
                            )
                    ),
                    ops(
                            put(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(dataNodesHistory)),
                            put(
                                    scaleUpTimerKey(zoneId),
                                    newScaleUpTimer.timestamp.longValue() > timestamp.longValue() ? DistributionZoneTimerSerializer.serialize(newScaleUpTimer) : DEFAULT_TIMER
                            ),
                            put(
                                    scaleDownTimerKey(zoneId),
                                    newScaleDownTimer.timestamp.longValue() > timestamp.longValue() ? DistributionZoneTimerSerializer.serialize(newScaleDownTimer) : DEFAULT_TIMER
                            )
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

            DataNodesHistory newHistory = new DataNodesHistory(new TreeMap<>(dataNodesHistory.history));
            newHistory.history.put(scaleUpTimer.timestamp, currentDataNodes.getSecond());

            return iif(
                    and(
                            value(dataNodesHistoryKey(zoneId)).eq(DataNodesHistorySerializer.serialize(dataNodesHistory)),
                            value(scaleUpTimerKey(zoneId)).eq(DistributionZoneTimerSerializer.serialize(scaleUpTimer0))
                    ),
                    ops(
                            put(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(newHistory)),
                            put(scaleUpTimerKey(zoneId), DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER))
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

            DataNodesHistory newHistory = new DataNodesHistory(new TreeMap<>(dataNodesHistory.history));
            newHistory.history.put(scaleDownTimer.timestamp, currentDataNodes.getSecond());

            return iif(
                    and(
                            value(dataNodesHistoryKey(zoneId)).eq(DataNodesHistorySerializer.serialize(dataNodesHistory)),
                            value(scaleDownTimerKey(zoneId)).eq(DistributionZoneTimerSerializer.serialize(scaleDownTimer0))
                    ),
                    ops(
                            put(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer.serialize(newHistory)),
                            put(scaleDownTimerKey(zoneId), DistributionZoneTimerSerializer.serialize(DEFAULT_TIMER))
                    ).yield(true),
                    ops().yield(false)
            );
        });

        rescheduleScaleDown(zoneDescriptor.dataNodesAutoAdjustScaleDown(), runnable, zoneId);

        return completedFuture(null);
    }

    public synchronized void rescheduleScaleUp(long delay, Runnable runnable, int zoneId) {

    }

    public synchronized void rescheduleScaleDown(long delay, Runnable runnable, int zoneId) {

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
            currentDataNodes = scaleUpTimer.nodes;
        }

        if (sdtt > cdnt && sdtt > sutt && sdtt <= timestampLong) {
            currentDataNodesTimestamp = scaleDownTimer.timestamp;
            currentDataNodes = scaleDownTimer.nodes;
        }

        return pair(currentDataNodesTimestamp, currentDataNodes);
    }

    public void onFilterChanged() {

    }

    public void onAutoAdjustAlteration() {

    }

    public CompletableFuture<Set<String>> dataNodes(int zoneId, HybridTimestamp timestamp) {

    }

    private DataNodesHistory dataNodesHistory(int zoneId) {
        return ofNullable(getValueFromMetaStorage(dataNodesHistoryKey(zoneId), DataNodesHistorySerializer::deserialize))
                .orElse(new DataNodesHistory(new TreeMap<>()));
    }

    private DistributionZoneTimer scaleUpTimer(int zoneId) {
        return ofNullable(getValueFromMetaStorage(scaleUpTimerKey(zoneId), DistributionZoneTimerSerializer::deserialize))
                .orElse(DEFAULT_TIMER);
    }

    private DistributionZoneTimer scaleDownTimer(int zoneId) {
        return ofNullable(getValueFromMetaStorage(scaleDownTimerKey(zoneId), DistributionZoneTimerSerializer::deserialize))
                .orElse(DEFAULT_TIMER);
    }

    @Nullable
    private <T> T getValueFromMetaStorage(ByteArray key, Function<byte[], T> deserializer) {
        Entry e = metaStorageManager.getLocally(key);

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
