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

import static org.apache.ignite.internal.metastorage.dsl.CompoundCondition.and;
import static org.apache.ignite.internal.metastorage.dsl.CompoundCondition.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;

import java.util.Set;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;

/**
 * Util class for Distribution Zones flow.
 */
class DistributionZonesUtil {
    /** Key prefix for zone's data nodes. */
    private static final String DISTRIBUTION_ZONE_DATA_NODES_PREFIX = "distributionZone.dataNodes.";

    private static final String DISTRIBUTION_ZONE_SCALE_UP_CHANGE_TRIGGER_PREFIX = "distributionZone.scaleUp.change.trigger.";

    private static final String DISTRIBUTION_ZONE_SCALE_DOWN_CHANGE_TRIGGER_PREFIX = "distributionZone.scaleDown.change.trigger.";

    /** Key prefix for zones' logical topology nodes. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY = "distributionZones.logicalTopology";

    /** Key prefix for zones' logical topology version. */
    private static final String DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION = "distributionZones.logicalTopologyVersion";

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY}. */
    private static final ByteArray DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_KEY = new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY);

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION}. */
    private static final ByteArray DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION_KEY =
            new ByteArray(DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION);

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_PREFIX}. */
    static ByteArray zoneDataNodesKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_PREFIX + zoneId);
    }

    /** */
    static ByteArray zoneScaleUpChangeTriggerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_UP_CHANGE_TRIGGER_PREFIX + zoneId);
    }

    /** */
    static ByteArray zoneScaleDownChangeTriggerKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_SCALE_DOWN_CHANGE_TRIGGER_PREFIX + zoneId);
    }

    /**
     * The key that represents logical topology nodes, needed for distribution zones. It is needed to store them in the metastore
     * to serialize data nodes changes triggered by topology changes and changes of distribution zones configurations.
     */
    static ByteArray zonesLogicalTopologyKey() {
        return DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_KEY;
    }

    /**
     * The key needed for processing the events about logical topology changes.
     * Needed for the defencing against stale updates of logical topology nodes.
     */
    static ByteArray zonesLogicalTopologyVersionKey() {
        return DISTRIBUTION_ZONES_LOGICAL_TOPOLOGY_VERSION_KEY;
    }

    /**
     * Condition for updating {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} key.
     * Update only if the revision of the event is newer than value in that trigger key.
     *
     * @param revision Event revision.
     * @return Update condition.
     */
    static CompoundCondition triggerKeyCondition(long revision, int zoneId) {
        return or(
                notExists(zoneScaleUpChangeTriggerKey(zoneId)),
                value(zoneScaleUpChangeTriggerKey(zoneId)).lt(ByteUtils.longToBytes(revision))
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
        return and(
                value(zoneScaleUpChangeTriggerKey(zoneId)).eq(ByteUtils.longToBytes(scaleUpTriggerRevision)),
                value(zoneScaleDownChangeTriggerKey(zoneId)).eq(ByteUtils.longToBytes(scaleDownTriggerRevision))
        );
    }

    /**
     * Updates data nodes value for a zone and set {@code revision} to {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @param logicalTopology Logical topology.
     * @return Update command for the meta storage.
     */
    static Update updateDataNodesAndTriggerKey(int zoneId, long revision, byte[] logicalTopology) {
        return ops(
                put(zoneDataNodesKey(zoneId), logicalTopology),
                put(zoneScaleUpChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision))
        ).yield(true);
    }

    static Update updateDataNodesAndScaleUpTriggerKey(int zoneId, long revision, byte[] nodes) {
        return ops(
                put(zoneDataNodesKey(zoneId), nodes),
                put(zoneScaleUpChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision))
        ).yield(true);
    }

    /**
     * Updates logical topology and logical topology version values for zones.
     *
     * @param logicalTopology Logical topology.
     * @param topologyVersion Logical topology version.
     * @return Update command for the meta storage.
     */
    static Update updateLogicalTopologyAndVersion(Set<String> logicalTopology, long topologyVersion) {
        return ops(
                put(zonesLogicalTopologyVersionKey(), ByteUtils.longToBytes(topologyVersion)),
                put(zonesLogicalTopologyKey(), ByteUtils.toBytes(logicalTopology))
        ).yield(true);
    }

    /**
     * Sets {@code revision} to {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)}.
     *
     * @param revision Revision of the event.
     * @return Update command for the meta storage.
     */
    static Update updateTriggerKey(long revision, int zoneId) {
        return ops(put(zoneScaleUpChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision))).yield(true);
    }

    /**
     * Deletes data nodes value for a zone and set {@code revision} to {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @return Update command for the meta storage.
     */
    static Update deleteDataNodesKeyAndUpdateTriggerKey(int zoneId, long revision) {
        return ops(
                remove(zoneDataNodesKey(zoneId)),
                put(zoneScaleUpChangeTriggerKey(zoneId), ByteUtils.longToBytes(revision))
        ).yield(true);
    }
}
