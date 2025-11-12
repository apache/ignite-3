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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.uuidToBytes;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DistributionZoneTimerSerializer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Util class for Distribution Zones flow.
 */
public class DistributionZonesUtil {
    /** Key prefix for distribution zone's keys. */
    private static final String DISTRIBUTION_ZONE_PREFIX = "distributionZone.";

    /** Key prefix for zone's data nodes and trigger keys. */
    private static final String DISTRIBUTION_ZONE_DATA_NODES_PREFIX = DISTRIBUTION_ZONE_PREFIX + "dataNodes.";

    /** Key prefix for zone's data nodes history. */
    public static final String DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX = DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "history.";

    /** Key prefix for zone's data nodes. Deprecated, preserved for backward compatibility. */
    @Deprecated
    public static final String DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX = DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "value.";

    /** Key prefix for zone's data nodes history, in {@code byte[]} representation. */
    public static final byte[] DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES =
            DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX.getBytes(StandardCharsets.UTF_8);

    /** Key prefix for zone's scale up timer. */
    public static final String DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX = DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "scaleUpTimer.";

    /** Key prefix for zone's scale up timer, in {@code byte[]} representation. */
    static final byte[] DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX_BYTES =
            DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX.getBytes(StandardCharsets.UTF_8);

    /** Key prefix for zone's scale down timer. */
    public static final String DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX = DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "scaleDownTimer.";

    /** Key prefix for zone's scale down timer, in {@code byte[]} representation. */
    static final byte[] DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX_BYTES =
            DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX.getBytes(StandardCharsets.UTF_8);

    /** Key prefix for zones' logical topology nodes and logical topology version. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX = "distributionZones.logicalTopology.";

    /** Key value for zones' nodes' attributes in vault. */
    private static final String DISTRIBUTION_ZONES_NODES_ATTRIBUTES = "distributionZones.nodesAttributes";

    /** Key value for zones' recoverable state revision. */
    private static final String DISTRIBUTION_ZONES_RECOVERABLE_STATE_REVISION = "distributionZones.recoverableStateRevision";

    /** Key value for the last handled logical topology by Distribution zone manager. */
    private static final String DISTRIBUTION_ZONES_LAST_HANDLED_TOPOLOGY = "distributionZones.lastHandledTopology";

    /** Key prefix for zones' logical topology nodes. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY = DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX + "nodes";

    /** Key prefix for zones' logical topology version. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION = DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX + "version";

    /** Key prefix for zones' logical topology cluster ID. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_CLUSTER_ID = DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX + "clusterId";

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY}. */
    private static final ByteArray DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_KEY = new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY);

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_NODES_ATTRIBUTES}.
     * Deprecated, preserved for backward compatibility.
     */
    @Deprecated
    private static final ByteArray DISTRIBUTION_ZONES_NODES_ATTRIBUTES_KEY = new ByteArray(DISTRIBUTION_ZONES_NODES_ATTRIBUTES);

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_RECOVERABLE_STATE_REVISION}. */
    private static final ByteArray DISTRIBUTION_ZONES_RECOVERABLE_STATE_REVISION_KEY =
            new ByteArray(DISTRIBUTION_ZONES_RECOVERABLE_STATE_REVISION);

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LAST_HANDLED_TOPOLOGY}. */
    private static final ByteArray DISTRIBUTION_ZONES_LAST_HANDLED_TOPOLOGY_KEY =
            new ByteArray(DISTRIBUTION_ZONES_LAST_HANDLED_TOPOLOGY);

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION}. */
    private static final ByteArray DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION_KEY =
            new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION);

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_CLUSTER_ID}. */
    private static final ByteArray DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_CLUSTER_ID_KEY =
            new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_CLUSTER_ID);

    /**
     * Internal property that determines partition group members reset timeout after the partition group majority loss.
     *
     * <p>Default value is {@link #PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE}.</p>
     */
    public static final String PARTITION_DISTRIBUTION_RESET_TIMEOUT = "partitionDistributionResetTimeout";

    /** Default value for the {@link #PARTITION_DISTRIBUTION_RESET_TIMEOUT}. */
    static final int PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE = 0;

    /**
     * Internal property that determines delay between unsuccessful trial of a rebalance and a new trial, ms.
     *
     * <p>Default value is {@link #REBALANCE_RETRY_DELAY_DEFAULT}.</p>
     */
    public static final String REBALANCE_RETRY_DELAY_MS = "rebalanceRetryDelay";

    /** Default value for the {@link #REBALANCE_RETRY_DELAY_MS}. */
    public static final int REBALANCE_RETRY_DELAY_DEFAULT = 200;

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX}.
     *
     * @return ByteArray representation.
     */
    public static ByteArray zoneDataNodesHistoryPrefix() {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX}.
     * Preserved for backward compatibility.
     *
     * @param zoneId Zone id.
     * @return ByteArray representation.
     */
    @Deprecated
    public static ByteArray zoneDataNodesKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX + zoneId);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX}.
     *
     * @param zoneId Zone id.
     * @return ByteArray representation.
     */
    public static ByteArray zoneDataNodesHistoryKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX + zoneId);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX}.
     *
     * @return ByteArray representation.
     */
    public static ByteArray zoneScaleUpTimerPrefix() {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX}.
     *
     * @param zoneId Zone id.
     * @return ByteArray representation.
     */
    public static ByteArray zoneScaleUpTimerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX + zoneId);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX}.
     *
     * @return ByteArray representation.
     */
    public static ByteArray zoneScaleDownTimerPrefix() {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX}.
     *
     * @param zoneId Zone id.
     * @return ByteArray representation.
     */
    public static ByteArray zoneScaleDownTimerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX + zoneId);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX}.
     *
     * @return ByteArray representation.
     */
    public static ByteArray zonesLogicalTopologyPrefix() {
        return new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX);
    }

    /**
     * The key that represents logical topology nodes, needed for distribution zones. It is needed to store them in the metastore
     * to serialize data nodes changes triggered by topology changes and changes of distribution zones configurations.
     */
    public static ByteArray zonesLogicalTopologyKey() {
        return DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_KEY;
    }

    /**
     * The key needed for processing the events about logical topology changes.
     * Needed for the defencing against stale updates of logical topology nodes.
     */
    public static ByteArray zonesLogicalTopologyVersionKey() {
        return DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION_KEY;
    }

    /**
     * The key needed for processing the events about logical topology changes.
     * Needed for the defencing against stale updates of logical topology nodes ({@link #zonesLogicalTopologyVersionKey()}
     * alone is not enough as version might be reset to 1 when a cluster reset happens; this key allows to distinguish between
     * two events about version=1).
     */
    public static ByteArray zonesLogicalTopologyClusterIdKey() {
        return DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_CLUSTER_ID_KEY;
    }

    /**
     * The key that represents nodes' attributes in Meta Storage. Deprecated, preserved for backward compatibility.
     */
    @Deprecated
    public static ByteArray zonesNodesAttributes() {
        return DISTRIBUTION_ZONES_NODES_ATTRIBUTES_KEY;
    }

    /**
     * The key represents zones' recoverable state revision. This is the revision of the event that triggered saving the recoverable state
     * of Distribution Zone Manager in Meta Storage.
     */
    public static ByteArray zonesRecoverableStateRevision() {
        return DISTRIBUTION_ZONES_RECOVERABLE_STATE_REVISION_KEY;
    }

    /**
     * The key represents the last handled logical topology by Distribution zone manager.
     */
    public static ByteArray zonesLastHandledTopology() {
        return DISTRIBUTION_ZONES_LAST_HANDLED_TOPOLOGY_KEY;
    }

    /**
     * Condition fot updating recoverable state of Distribution zone manager.
     * Update only if the revision of the event is newer than value in that trigger key.
     *
     * @param revision Event revision.
     * @return Update condition.
     */
    static CompoundCondition conditionForRecoverableStateChanges(long revision) {
        return or(
                notExists(zonesRecoverableStateRevision()),
                value(zonesRecoverableStateRevision()).lt(longToBytesKeepingOrder(revision))
        );
    }

    /**
     * Updates logical topology and its version values for zones.
     *
     * @param logicalTopology Logical topology snapshot.
     * @return Update command for the meta storage.
     */
    static Update updateLogicalTopologyAndVersion(LogicalTopologySnapshot logicalTopology) {
        return updateLogicalTopologyAndVersionAndMaybeClusterId(logicalTopology, false);
    }

    /**
     * Updates logical topology, its version and cluster ID values for zones.
     *
     * @param logicalTopology Logical topology snapshot.
     * @return Update command for the meta storage.
     */
    static Update updateLogicalTopologyAndVersionAndClusterId(LogicalTopologySnapshot logicalTopology) {
        return updateLogicalTopologyAndVersionAndMaybeClusterId(logicalTopology, true);
    }

    private static Update updateLogicalTopologyAndVersionAndMaybeClusterId(
            LogicalTopologySnapshot logicalTopology,
            boolean updateClusterId
    ) {
        Set<NodeWithAttributes> topologyFromCmg = logicalTopology.nodes().stream()
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        List<Operation> operations = new ArrayList<>();

        operations.add(put(zonesLogicalTopologyVersionKey(), longToBytesKeepingOrder(logicalTopology.version())));
        operations.add(put(
                zonesLogicalTopologyKey(),
                LogicalTopologySetSerializer.serialize(topologyFromCmg)
        ));
        if (updateClusterId) {
            operations.add(put(zonesLogicalTopologyClusterIdKey(), uuidToBytes(logicalTopology.clusterId())));
        }

        return ops(operations.toArray(Operation[]::new)).yield(true);
    }

    /**
     * Returns a set of data nodes retrieved from data nodes map, which value is more than 0.
     *
     * @param dataNodesMap This map has the following structure: node name is mapped to an integer,
     *                     an integer represents counter for node joining or leaving the topology.
     *                     Joining increases the counter, leaving decreases.
     * @return Returns a set of data nodes retrieved from data nodes map, which value is more than 0.
     */
    public static Set<Node> dataNodes(Map<Node, Integer> dataNodesMap) {
        return dataNodesMap.entrySet().stream().filter(e -> e.getValue() > 0).map(Map.Entry::getKey).collect(toSet());
    }

    /**
     * Returns a set of data nodes retrieved from data nodes map, which value is more than 0.
     *
     * @param dataNodes Data nodes with attributes set.
     * @return Returns a set of data nodes retrieved from data nodes with attributes set.
     */
    public static Set<Node> dataNodes(Set<NodeWithAttributes> dataNodes) {
        return dataNodes.stream().map(NodeWithAttributes::node).collect(toSet());
    }

    /**
     * Parse the data nodes from bytes.
     *
     * @param dataNodesBytes Data nodes bytes.
     * @param timestamp Timestamp.
     * @return Set of nodes.
     */
    @Nullable
    public static Set<NodeWithAttributes> parseDataNodes(byte[] dataNodesBytes, HybridTimestamp timestamp) {
        if (dataNodesBytes == null) {
            return null;
        }

        DataNodesHistory dataNodesHistory = DataNodesHistorySerializer.deserialize(dataNodesBytes);

        return dataNodesHistory.dataNodesForTimestamp(timestamp).dataNodes();
    }

    /**
     * Deserialize the set of nodes with attributes from the given serialized bytes.
     *
     * @param bytes Serialized set of nodes with attributes.
     * @return Set of nodes with attributes.
     */
    public static Set<NodeWithAttributes> deserializeLogicalTopologySet(byte[] bytes) {
        return LogicalTopologySetSerializer.deserialize(bytes);
    }

    static Map<UUID, NodeWithAttributes> deserializeNodesAttributes(byte[] bytes) {
        return NodesAttributesSerializer.deserialize(bytes);
    }

    /**
     * Meta storage entries to {@link DataNodesHistoryContext}.
     *
     * @param entries Entries.
     * @return DataNodeHistoryContext.
     */
    @Nullable
    public static DistributionZonesUtil.DataNodesHistoryContext dataNodeHistoryContextFromValues(Collection<Entry> entries) {
        DataNodesHistory dataNodesHistory = null;
        DistributionZoneTimer scaleUpTimer = null;
        DistributionZoneTimer scaleDownTimer = null;

        for (Entry e : entries) {
            if (e.empty()) {
                return null;
            }

            assert e != null && e.key() != null : "Unexpected entry: " + e;

            byte[] v = e.tombstone() ? null : e.value();

            if (startsWith(e.key(), DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES)) {
                dataNodesHistory = v == null ? null : DataNodesHistorySerializer.deserialize(v);
            } else if (startsWith(e.key(), DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX_BYTES)) {
                scaleUpTimer = v == null ? null : DistributionZoneTimerSerializer.deserialize(v);
            } else if (startsWith(e.key(), DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX_BYTES)) {
                scaleDownTimer = v == null ? null : DistributionZoneTimerSerializer.deserialize(v);
            }
        }

        return new DataNodesHistoryContext(dataNodesHistory, scaleUpTimer, scaleDownTimer);
    }

    /**
     * Check if {@code nodeAttributes} satisfy the {@code filter}.
     *
     * <p>Some examples:
     * <ol>
     *     <li>Node attributes: ("region" -> "US", "storage" -> "SSD"); filter: "$[?(@.region == 'US')]"; result: true</li>
     *     <li>Node attributes: ("region" -> "US"); filter: "$[?(@.storage == 'SSD' && @.region == 'US')]"; result: false</li>
     *     <li>Node attributes: ("region" -> "US"); filter: "$[?(@.storage == 'SSD']"; result: false</li>
     *     <li>Node attributes: ("region" -> "US"); filter: "$[?(@.storage != 'SSD']"; result: true</li>
     *     <li>Node attributes: ("region" -> "US", "dataRegionSize: 10); filter: "$[?(@.region == 'EU' || @.dataRegionSize > 5)]";
     *     result: true</li>
     * </ol>
     * Note, that in the example 4 we can see, that {@code JsonPath} threats missed 'storage' attribute as an attribute, that passes
     * {@code $[?(@.storage != 'SSD']}. If it is needed, that node without 'storage' does not pass a filter, it's needed to use EXISTS
     * logic for that attribute, like {@code $[?(@.storage && @.storage != 'SSD']}
     *
     * @param nodeAttributes Key value map of node's attributes.
     * @param filter Valid {@link JsonPath} filter of JSON fields.
     * @return True if {@code nodeAttributes} satisfy {@code filter}, false otherwise. Returns true if {@code nodeAttributes} is empty.
     */
    public static boolean filterNodeAttributes(Map<String, String> nodeAttributes, String filter) {
        if (filter.equals(DEFAULT_FILTER)) {
            return true;
        }
        // We need to convert numbers to Long objects, so they could be parsed to numbers in JSON.
        // nodeAttributes has String values of numbers because nodeAttributes come from configuration,
        // but configuration does not support Object as a configuration value.
        Map<String, Object> convertedAttributes = nodeAttributes.entrySet().stream()
                .collect(
                        toMap(
                                Map.Entry::getKey,
                                e -> {
                                    long res;

                                    try {
                                        res = Long.parseLong(e.getValue());
                                    } catch (NumberFormatException ignored) {
                                        return e.getValue();
                                    }
                                    return res;
                                })
                );

        Configuration jsonPathCfg = new Configuration.ConfigurationBuilder()
                .options(Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
                .build();

        List<Map<String, Object>> res = JsonPath.using(jsonPathCfg).parse(convertedAttributes).read(filter);

        return !res.isEmpty();
    }

    /**
     * Filters storage profiles.
     *
     * @param node Node with storage profile attributes.
     * @param zoneStorageProfiles Zone's storage profiles.
     * @return True, if matches, false otherwise.
     */
    public static boolean filterStorageProfiles(
            NodeWithAttributes node,
            List<CatalogStorageProfileDescriptor> zoneStorageProfiles
    ) {
        if (node.storageProfiles() == null) {
            return false;
        }

        List<String> zoneStorageProfilesNames = zoneStorageProfiles.stream()
                .map(CatalogStorageProfileDescriptor::storageProfile)
                .collect(toList());

        return new HashSet<>(node.storageProfiles()).containsAll(zoneStorageProfilesNames);
    }

    /**
     * Filters {@code dataNodes} according to the provided filter and storage profiles from {@code zoneDescriptor}.
     *
     * @param dataNodes Data nodes with attributes.
     * @param zoneDescriptor Zone descriptor.
     * @return Filtered data nodes.
     */
    public static Set<NodeWithAttributes> filterDataNodes(
            Set<NodeWithAttributes> dataNodes,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        return dataNodes.stream()
                .filter(n -> filterNodeAttributes(n.userAttributes(), zoneDescriptor.filter()))
                .filter(n -> filterStorageProfiles(n, zoneDescriptor.storageProfiles().profiles()))
                .collect(toSet());
    }

    /**
     * Parse string representation of storage profiles.
     *
     * @param storageProfiles String representation of storage profiles.
     * @return List of storage profile params
     */
    public static List<StorageProfileParams> parseStorageProfiles(String storageProfiles) {
        List<String> items = Arrays.asList(storageProfiles.split("\\s*,\\s*"));

        return items.stream()
                .map(p -> StorageProfileParams.builder().storageProfile(p).build())
                .collect(toList());
    }

    /**
     * Create an executor for the zone manager.
     * Used a striped thread executor to avoid concurrent executing several tasks for the same zone.
     * ScheduledThreadPoolExecutor guarantee that tasks scheduled for exactly the same
     * execution time are enabled in first-in-first-out (FIFO) order of submission.
     *
     * @param concurrencyLvl Number of threads.
     * @param threadFactory Named thread factory.
     * @return Executor.
     */
    static StripedScheduledThreadPoolExecutor createZoneManagerExecutor(int concurrencyLvl, IgniteThreadFactory threadFactory) {
        return new StripedScheduledThreadPoolExecutor(
                concurrencyLvl,
                threadFactory,
                new ThreadPoolExecutor.DiscardPolicy()
        );
    }

    public static Set<String> nodeNames(Set<NodeWithAttributes> nodes) {
        return nodes.stream().map(NodeWithAttributes::nodeName).collect(toSet());
    }

    /**
     * Class representing data nodes' related values in Meta Storage.
     */
    public static class DataNodesHistoryContext {
        @Nullable
        private final DataNodesHistory dataNodesHistory;

        @Nullable
        private final DistributionZoneTimer scaleUpTimer;

        @Nullable
        private final DistributionZoneTimer scaleDownTimer;

        DataNodesHistoryContext(
                @Nullable DataNodesHistory dataNodesHistory,
                @Nullable DistributionZoneTimer scaleUpTimer,
                @Nullable DistributionZoneTimer scaleDownTimer
        ) {
            this.dataNodesHistory = dataNodesHistory;
            this.scaleUpTimer = scaleUpTimer;
            this.scaleDownTimer = scaleDownTimer;
        }

        /**
         * Data nodes history.
         *
         * @return Data nodes history.
         */
        public DataNodesHistory dataNodesHistory() {
            assert dataNodesHistory != null : "Data nodes history were not initialized.";

            return dataNodesHistory;
        }

        /**
         * Scale up timer.
         *
         * @return Scale up timer.
         */
        public DistributionZoneTimer scaleUpTimer() {
            assert scaleUpTimer != null : "Scale up timer was not initialized.";

            return scaleUpTimer;
        }

        /**
         * Scale up timer present.
         *
         * @return Scale up timer present.
         */
        @TestOnly
        public boolean scaleUpTimerPresent() {
            return scaleUpTimer != null;
        }

        /**
         * Scale down timer present.
         *
         * @return Scale down timer present.
         */
        @TestOnly
        public boolean scaleDownTimerPresent() {
            return scaleDownTimer != null;
        }

        /**
         * Scale down timer.
         *
         * @return Scale down timer.
         */
        public DistributionZoneTimer scaleDownTimer() {
            assert scaleDownTimer != null : "Scale down timer was not initialized.";

            return scaleDownTimer;
        }
    }
}
