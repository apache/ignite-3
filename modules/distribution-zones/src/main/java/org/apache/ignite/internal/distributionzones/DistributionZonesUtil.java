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

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Util class for Distribution Zones flow.
 */
public class DistributionZonesUtil {
    /** Key prefix for zone's data nodes and trigger keys. */
    private static final String DISTRIBUTION_ZONE_DATA_NODES_PREFIX = "distributionZone.dataNodes.";

    /** Key prefix for zone's data nodes. */
    private static final String DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX = DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "value.";

    /** Key prefix for zone's scale up change trigger key. */
    private static final String DISTRIBUTION_ZONE_SCALE_UP_CHANGE_TRIGGER_PREFIX =
            DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "scaleUpChangeTrigger.";

    /** Key prefix for zone's scale down change trigger key. */
    private static final String DISTRIBUTION_ZONE_SCALE_DOWN_CHANGE_TRIGGER_PREFIX =
            DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "scaleDownChangeTrigger.";

    /** Key prefix for zones' logical topology nodes and logical topology version. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX = "distributionZones.logicalTopology.";

    /** Key prefix for zones' logical topology nodes. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY = DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX + "nodes";

    /** Key prefix for zones' logical topology version. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION = DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX + "version";

    /** Key prefix, needed for processing the event about zone's update was triggered only once. */
    private static final String DISTRIBUTION_ZONES_CHANGE_TRIGGER_KEY_PREFIX = "distributionZones.change.trigger.";

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY}. */
    private static final ByteArray DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_KEY = new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY);

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION}. */
    private static final ByteArray DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION_KEY =
            new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION);

    /**
     * The initial value of trigger revision in case when it is not initialized in the meta storage.
     * The trigger revision in the meta storage can be uninitialized for the default distribution zone.
     */
    private static final long INITIAL_TRIGGER_REVISION_VALUE = 0;

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_PREFIX}. */
    private static final ByteArray DISTRIBUTION_ZONES_DATA_NODES_KEY =
            new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_PREFIX);

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX}.
     *
     * @param zoneId Zone id.
     * @return ByteArray representation.
     */
    public static ByteArray zoneDataNodesKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX + zoneId);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX}.
     *
     * @return ByteArray representation.
     */
    public static ByteArray zoneDataNodesKey() {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX);
    }

    /**
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX}.
     *
     * @return ByteArray representation.
     */
    public static ByteArray zoneLogicalTopologyPrefix() {
        return new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX);
    }

    /**
     * Extract zone id from a distribution zone data nodes key.
     *
     * @param key Key.
     * @return Zone id.
     */
    public static int extractZoneId(byte[] key) {
        var strKey = new String(key, StandardCharsets.UTF_8);

        return Integer.parseInt(strKey.substring(DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX.length()));
    }

    /**
     * The key needed for processing an event about zone's creation and deletion.
     * With this key we can be sure that event was triggered only once.
     */
    public static ByteArray zonesChangeTriggerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONES_CHANGE_TRIGGER_KEY_PREFIX + zoneId);
    }

    /**
     * The key needed for processing an event about zone's data node propagation on scale up.
     * With this key we can be sure that event was triggered only once.
     */
    public static ByteArray zoneScaleUpChangeTriggerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_UP_CHANGE_TRIGGER_PREFIX + zoneId);
    }

    /**
     * The key prefix needed for processing an event about zone's data node propagation on scale up.
     */
    public static ByteArray zoneScaleUpChangeTriggerKey() {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_UP_CHANGE_TRIGGER_PREFIX);
    }

    /**
     * The key needed for processing an event about zone's data node propagation on scale down.
     * With this key we can be sure that event was triggered only once.
     */
    public static ByteArray zoneScaleDownChangeTriggerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_DOWN_CHANGE_TRIGGER_PREFIX + zoneId);
    }

    /**
     * The key prefix needed for processing an event about zone's data node propagation on scale down.
     */
    public static ByteArray zoneScaleDownChangeTriggerKey() {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_DOWN_CHANGE_TRIGGER_PREFIX);
    }

    /**
     * The key that represents logical topology nodes, needed for distribution zones. It is needed to store them in the metastore
     * to serialize data nodes changes triggered by topology changes and changes of distribution zones configurations.
     */
    public static ByteArray zonesLogicalTopologyKey() {
        return DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_KEY;
    }

    /**
     * The key needed for processing the events about logical topology changes.
     * Needed for the defencing against stale updates of logical topology nodes.
     */
    public static ByteArray zonesLogicalTopologyVersionKey() {
        return DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION_KEY;
    }

    /**
     * The key prefix needed for processing an event about zone's data nodes.
     */
    static ByteArray zonesDataNodesPrefix() {
        return DISTRIBUTION_ZONES_DATA_NODES_KEY;
    }

    /**
     * Condition for updating {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} key.
     * Update only if the revision of the event is newer than value in that trigger key.
     *
     * @param revision Event revision.
     * @return Update condition.
     */
    static CompoundCondition triggerKeyConditionForZonesChanges(long revision, int zoneId) {
        return or(
                notExists(zonesChangeTriggerKey(zoneId)),
                value(zonesChangeTriggerKey(zoneId)).lt(ByteUtils.longToBytes(revision))
        );
    }

    /**
     * Condition for updating {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} key.
     * Update only if the revision of the event is newer than value in that trigger key.
     *
     * @param scaleUpTriggerRevision Trigger revision of scale up.
     * @param scaleDownTriggerRevision Trigger revision of scale down.
     * @param zoneId Zone id.
     * @return Update condition.
     */
    static CompoundCondition triggerScaleUpScaleDownKeysCondition(long scaleUpTriggerRevision, long scaleDownTriggerRevision,  int zoneId) {
        SimpleCondition scaleUpCondition;

        if (scaleUpTriggerRevision != INITIAL_TRIGGER_REVISION_VALUE) {
            scaleUpCondition = value(zoneScaleUpChangeTriggerKey(zoneId)).eq(ByteUtils.longToBytes(scaleUpTriggerRevision));
        } else {
            scaleUpCondition = notExists(zoneScaleUpChangeTriggerKey(zoneId));
        }

        SimpleCondition scaleDownCondition;

        if (scaleDownTriggerRevision != INITIAL_TRIGGER_REVISION_VALUE) {
            scaleDownCondition = value(zoneScaleDownChangeTriggerKey(zoneId)).eq(ByteUtils.longToBytes(scaleDownTriggerRevision));
        } else {
            scaleDownCondition = notExists(zoneScaleDownChangeTriggerKey(zoneId));
        }

        return and(scaleUpCondition, scaleDownCondition);
    }

    /**
     * Updates data nodes value for a zone and set {@code revision} to {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @param nodes Data nodes.
     * @return Update command for the meta storage.
     */
    static Update updateDataNodesAndScaleUpTriggerKey(int zoneId, long revision, byte[] nodes) {
        return ops(
                put(zoneDataNodesKey(zoneId), nodes),
                put(zoneScaleUpChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision))
        ).yield(true);
    }

    static Update updateDataNodesAndScaleDownTriggerKey(int zoneId, long revision, byte[] nodes) {
        return ops(
                put(zoneDataNodesKey(zoneId), nodes),
                put(zoneScaleDownChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision))
        ).yield(true);
    }


    /**
     * Updates data nodes value for a zone and set {@code revision} to {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)},
     * {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)} and {@link DistributionZonesUtil#zonesChangeTriggerKey(int)}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @param nodes Data nodes.
     * @return Update command for the meta storage.
     */
    static Update updateDataNodesAndTriggerKeys(int zoneId, long revision, byte[] nodes) {
        return ops(
                put(zoneDataNodesKey(zoneId), nodes),
                put(zoneScaleUpChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision)),
                put(zoneScaleDownChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision)),
                put(zonesChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision))
        ).yield(true);
    }

    /**
     * Deletes data nodes, {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)},
     * {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)} values for a zone. Also sets {@code revision} to
     * {@link DistributionZonesUtil#zonesChangeTriggerKey(int)}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @return Update command for the meta storage.
     */
    static Update deleteDataNodesAndUpdateTriggerKeys(int zoneId, long revision) {
        return ops(
                remove(zoneDataNodesKey(zoneId)),
                remove(zoneScaleUpChangeTriggerKey(zoneId)),
                remove(zoneScaleDownChangeTriggerKey(zoneId)),
                put(zonesChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision))
        ).yield(true);
    }

    /**
     * Updates logical topology and logical topology version values for zones.
     *
     * @param logicalTopology Logical topology.
     * @param topologyVersion Logical topology version.
     * @return Update command for the meta storage.
     */
    public static Update updateLogicalTopologyAndVersion(Set<LogicalNode> logicalTopology, long topologyVersion) {
        Set<NodeWithAttributes> topologyFromCmg = logicalTopology.stream()
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.nodeAttributes()))
                .collect(toSet());

        return ops(
                put(zonesLogicalTopologyVersionKey(), ByteUtils.longToBytes(topologyVersion)),
                put(zonesLogicalTopologyKey(), ByteUtils.toBytes(topologyFromCmg))
        ).yield(true);
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
     * Returns a map from a set of data nodes. This map has the following structure: node is mapped to integer,
     * integer represents how often node joined or leaved topology. In this case, set of nodes is interpreted as nodes
     * that joined topology, so all mappings will be node -> 1.
     *
     * @param dataNodes Set of data nodes.
     * @return Returns a map from a set of data nodes.
     */
    public static Map<Node, Integer> toDataNodesMap(Set<Node> dataNodes) {
        Map<Node, Integer> dataNodesMap = new HashMap<>();

        dataNodes.forEach(n -> dataNodesMap.merge(n, 1, Integer::sum));

        return dataNodesMap;
    }

    /**
     * Returns data nodes from the meta storage entry or empty map if the value is null.
     *
     * @param dataNodesEntry Meta storage entry with data nodes.
     * @return Data nodes.
     */
    static Map<Node, Integer> extractDataNodes(Entry dataNodesEntry) {
        if (!dataNodesEntry.empty()) {
            return fromBytes(dataNodesEntry.value());
        } else {
            return emptyMap();
        }
    }

    /**
     * Returns a trigger revision from the meta storage entry or {@link INITIAL_TRIGGER_REVISION_VALUE} if the value is null.
     *
     * @param revisionEntry Meta storage entry with data nodes.
     * @return Revision.
     */
    static long extractChangeTriggerRevision(Entry revisionEntry) {
        if (!revisionEntry.empty()) {
            return bytesToLong(revisionEntry.value());
        } else {
            return INITIAL_TRIGGER_REVISION_VALUE;
        }
    }

    /**
     * Finds a zone configuration from zones configuration by its id.
     *
     * @param dstZnsCfg Distribution zones config.
     * @param zoneId Id of zone.
     * @return Zone configuration with appropriate zone id.
     */
    public static DistributionZoneConfiguration getZoneById(DistributionZonesConfiguration dstZnsCfg, int zoneId) {
        if (zoneId == DEFAULT_ZONE_ID) {
            return dstZnsCfg.defaultDistributionZone();
        }

        for (UUID id : dstZnsCfg.distributionZones().internalIds()) {
            DistributionZoneConfiguration distributionZoneConfiguration = dstZnsCfg.distributionZones().get(id);

            assert distributionZoneConfiguration != null;

            if (distributionZoneConfiguration.zoneId().value().equals(zoneId)) {
                return distributionZoneConfiguration;
            }
        }

        throw new DistributionZoneNotFoundException(zoneId);
    }

    /**
     * Checks if the zone with specified id exists.
     *
     * @param dstZnsCfg Distribution zones config.
     * @param zoneId Id of zone.
     * @return {@code true} if the zone exists. {@code false} if the zone doesn't exist.
     */
    static boolean isZoneExist(DistributionZonesConfiguration dstZnsCfg, int zoneId) {
        try {
            getZoneById(dstZnsCfg, zoneId);

            return true;
        } catch (DistributionZoneNotFoundException e) {
            return false;
        }
    }

    /**
     * Check if a passed filter is a valid {@link JsonPath} query.
     *
     * @param filter Filter.
     * @return {@code null} if the passed filter is a valid filter, string with the error message otherwise.
     */
    public static @Nullable String validate(String filter) {
        try {
            JsonPath.compile(filter);
        } catch (InvalidPathException e) {
            if (e.getMessage() != null) {
                return e.getMessage();
            } else {
                return "Unknown JsonPath compilation error.";
            }
        }

        return null;
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
    public static boolean filter(Map<String, String> nodeAttributes, String filter) {
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

        List<Map<String, Object>> res = JsonPath.read(convertedAttributes, filter);

        return !res.isEmpty();
    }

    /**
     * Filters {@code dataNodes} according to the provided {@code filter}.
     * Nodes' attributes are taken from {@code nodesAttributes} map.
     *
     * @param dataNodes Data nodes.
     * @param filter Filter for data nodes.
     * @param nodesAttributes Nodes' attributes which used for filtering.
     * @return Filtered data nodes.
     */
    public static Set<String> filterDataNodes(
            Set<Node> dataNodes,
            String filter,
            Map<String, Map<String, String>> nodesAttributes
    ) {
        return dataNodes.stream()
                .filter(n -> filter(nodesAttributes.get(n.nodeId()), filter))
                .map(Node::nodeName)
                .collect(toSet());
    }
}
