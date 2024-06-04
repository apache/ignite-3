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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notTombstone;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
import org.apache.ignite.internal.util.ByteUtils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Util class for Distribution Zones flow.
 */
public class DistributionZonesUtil {
    /** Key prefix for distribution zone's keys. */
    private static final String DISTRIBUTION_ZONE_PREFIX = "distributionZone.";

    /** Key prefix for zone's data nodes and trigger keys. */
    private static final String DISTRIBUTION_ZONE_DATA_NODES_PREFIX = "distributionZone.dataNodes.";

    /** Key prefix for zone's data nodes. */
    public static final String DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX = DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "value.";

    /** Key prefix for zone's scale up change trigger key. */
    private static final String DISTRIBUTION_ZONE_SCALE_UP_CHANGE_TRIGGER_PREFIX =
            DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "scaleUpChangeTrigger.";

    /** Key prefix for zone's scale down change trigger key. */
    private static final String DISTRIBUTION_ZONE_SCALE_DOWN_CHANGE_TRIGGER_PREFIX =
            DISTRIBUTION_ZONE_DATA_NODES_PREFIX + "scaleDownChangeTrigger.";

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

    /** Key prefix for zones' configurations in vault. */
    private static final String DISTRIBUTION_ZONES_VERSIONED_CONFIGURATION_VAULT = "vault." + DISTRIBUTION_ZONE_PREFIX
            + "versionedConfiguration.";

    /** Key prefix for zones' logical topology version. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION = DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_PREFIX + "version";

    /** Key prefix that represents {@link ZoneState#topologyAugmentationMap()}.*/
    private static final String DISTRIBUTION_ZONES_TOPOLOGY_AUGMENTATION_PREFIX = "distributionZones.topologyAugmentation.";

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY}. */
    private static final ByteArray DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_KEY = new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY);

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_NODES_ATTRIBUTES}. */
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

    /**
     * The initial value of trigger revision in case when it is not initialized in the meta storage.
     * It is possible because invoke to metastorage with the initialisation is async, and scale up/down propagation could be
     * propagated first. Initial value is -1, because for default zone, we initialise trigger keys with metastorage's applied revision,
     * which is 0 on a start.
     */
    private static final long INITIAL_TRIGGER_REVISION_VALUE = -1;

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
     * ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_VERSIONED_CONFIGURATION_VAULT}.
     *
     * @param zoneId Zone id.
     * @return ByteArray representation.
     */
    public static ByteArray zoneVersionedConfigurationKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONES_VERSIONED_CONFIGURATION_VAULT + zoneId);
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
     * The key needed for processing an event about zone's data node propagation on scale up.
     * With this key we can be sure that event was triggered only once.
     */
    public static ByteArray zoneScaleUpChangeTriggerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_UP_CHANGE_TRIGGER_PREFIX + zoneId);
    }

    /**
     * The key needed for processing an event about zone's data node propagation on scale down.
     * With this key we can be sure that event was triggered only once.
     */
    public static ByteArray zoneScaleDownChangeTriggerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_DOWN_CHANGE_TRIGGER_PREFIX + zoneId);
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
     * The key prefix needed for processing an event about zone's data nodes.
     */
    public static ByteArray zonesDataNodesPrefix() {
        return DISTRIBUTION_ZONES_DATA_NODES_KEY;
    }

    /**
     * The key that represents nodes' attributes in Meta Storage.
     */
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
     * The key that represents {@link ZoneState#topologyAugmentationMap()} in the Meta Storage.
     */
    static ByteArray zoneTopologyAugmentation(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONES_TOPOLOGY_AUGMENTATION_PREFIX + zoneId);
    }

    /**
     * Condition for creating all data nodes' related keys in Meta Storage. Condition passes only when
     * {@link DistributionZonesUtil#zoneDataNodesKey(int)} not exists and not a tombstone in the Meta Storage.
     *
     * @param zoneId Distribution zone id
     * @return Update condition.
     */
    static CompoundCondition conditionForZoneCreation(int zoneId) {
        return and(
                notExists(zoneDataNodesKey(zoneId)),
                notTombstone(zoneDataNodesKey(zoneId))
        );
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
     * Condition for removing all data nodes' related keys in Meta Storage.
     *
     * @param zoneId Distribution zone id
     * @return Update condition.
     */
    static SimpleCondition conditionForZoneRemoval(int zoneId) {
        return exists(zoneDataNodesKey(zoneId));
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
            scaleUpCondition = value(zoneScaleUpChangeTriggerKey(zoneId)).eq(longToBytesKeepingOrder(scaleUpTriggerRevision));
        } else {
            scaleUpCondition = notExists(zoneScaleUpChangeTriggerKey(zoneId));
        }

        SimpleCondition scaleDownCondition;

        if (scaleDownTriggerRevision != INITIAL_TRIGGER_REVISION_VALUE) {
            scaleDownCondition = value(zoneScaleDownChangeTriggerKey(zoneId)).eq(longToBytesKeepingOrder(scaleDownTriggerRevision));
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
                put(zoneScaleUpChangeTriggerKey(zoneId), longToBytesKeepingOrder(revision))
        ).yield(true);
    }

    /**
     * Updates data nodes value for a zone and set {@code revision} to {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @param nodes Data nodes.
     * @return Update command for the meta storage.
     */
    static Update updateDataNodesAndScaleDownTriggerKey(int zoneId, long revision, byte[] nodes) {
        return ops(
                put(zoneDataNodesKey(zoneId), nodes),
                put(zoneScaleDownChangeTriggerKey(zoneId), longToBytesKeepingOrder(revision))
        ).yield(true);
    }


    /**
     * Updates data nodes value for a zone and set {@code revision} to {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} and
     * {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @param nodes Data nodes.
     * @return Update command for the meta storage.
     */
    static Update updateDataNodesAndTriggerKeys(int zoneId, long revision, byte[] nodes) {
        return ops(
                put(zoneDataNodesKey(zoneId), nodes),
                put(zoneScaleUpChangeTriggerKey(zoneId), longToBytesKeepingOrder(revision)),
                put(zoneScaleDownChangeTriggerKey(zoneId), longToBytesKeepingOrder(revision))
        ).yield(true);
    }

    /**
     * Deletes data nodes, {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)},
     * {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)} values for a zone.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @return Update command for the meta storage.
     */
    static Update deleteDataNodesAndTriggerKeys(int zoneId, long revision) {
        return ops(
                remove(zoneDataNodesKey(zoneId)),
                remove(zoneScaleUpChangeTriggerKey(zoneId)),
                remove(zoneScaleDownChangeTriggerKey(zoneId))
        ).yield(true);
    }

    /**
     * Updates logical topology and logical topology version values for zones.
     *
     * @param logicalTopology Logical topology.
     * @param topologyVersion Logical topology version.
     * @return Update command for the meta storage.
     */
    static Update updateLogicalTopologyAndVersion(Set<LogicalNode> logicalTopology, long topologyVersion) {
        Set<NodeWithAttributes> topologyFromCmg = logicalTopology.stream()
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        return ops(
                put(zonesLogicalTopologyVersionKey(), longToBytesKeepingOrder(topologyVersion)),
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

    @Nullable
    public static Set<Node> parseDataNodes(byte[] dataNodesBytes) {
        return dataNodesBytes == null ? null : dataNodes(fromBytes(dataNodesBytes));
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
            return bytesToLongKeepingOrder(revisionEntry.value());
        } else {
            return INITIAL_TRIGGER_REVISION_VALUE;
        }
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
     * Nodes' attributes and storage profiles are taken from {@code nodesAttributes} map.
     *
     * @param dataNodes Data nodes.
     * @param zoneDescriptor Zone descriptor.
     * @param nodesAttributes Nodes' attributes which used for filtering.
     * @return Filtered data nodes.
     */
    public static Set<String> filterDataNodes(
            Set<Node> dataNodes,
            CatalogZoneDescriptor zoneDescriptor,
            Map<String, NodeWithAttributes> nodesAttributes
    ) {

        return dataNodes.stream()
                .filter(n -> filterNodeAttributes(nodesAttributes.get(n.nodeId()).userAttributes(), zoneDescriptor.filter()))
                .filter(n -> filterStorageProfiles(nodesAttributes.get(n.nodeId()), zoneDescriptor.storageProfiles().profiles()))
                .map(Node::nodeName)
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
     * @param namedThreadFactory Named thread factory.
     * @return Executor.
     */
    static StripedScheduledThreadPoolExecutor createZoneManagerExecutor(int concurrencyLvl, NamedThreadFactory namedThreadFactory) {
        return new StripedScheduledThreadPoolExecutor(
                concurrencyLvl,
                namedThreadFactory,
                new ThreadPoolExecutor.DiscardPolicy()
        );
    }


    /**
     * Returns list of table descriptors bound to the zone.
     *
     * @param zoneId Zone id.
     * @param catalogVersion Catalog version.
     * @param catalogService Catalog service
     * @return List of table descriptors from the zone.
     */
    public static List<CatalogTableDescriptor> findTablesByZoneId(int zoneId, int catalogVersion, CatalogService catalogService) {
        return catalogService.tables(catalogVersion).stream()
                .filter(table -> table.zoneId() == zoneId)
                .collect(toList());
    }

    /** Key prefix for zone's scale up change trigger key. */
    @TestOnly
    public static ByteArray zoneScaleUpChangeTriggerKeyPrefix() {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_UP_CHANGE_TRIGGER_PREFIX);
    }

    /** Key prefix for zone's scale down change trigger key. */
    @TestOnly
    public static ByteArray zoneScaleDownChangeTriggerKeyPrefix() {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_DOWN_CHANGE_TRIGGER_PREFIX);
    }
}
