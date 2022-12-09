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

import static org.apache.ignite.internal.metastorage.client.CompoundCondition.or;
import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.value;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;

import org.apache.ignite.internal.metastorage.client.CompoundCondition;
import org.apache.ignite.internal.metastorage.client.Update;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;

/**
 * Util class for Distribution Zones flow.
 */
class DistributionZonesUtil {
    /** Key prefix for zone's data nodes. */
    private static final String DISTRIBUTION_ZONE_DATA_NODES_PREFIX = "distributionZone.dataNodes.";

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_PREFIX}. */
    static ByteArray zoneDataNodesKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_PREFIX + zoneId);
    }

    /**
     * The key, needed for processing the event about zones' update was triggered only once.
     */
    static ByteArray zonesChangeTriggerKey() {
        return new ByteArray("distributionZones.change.trigger");
    }

    /**
     * Condition for updating {@link DistributionZonesUtil#zonesChangeTriggerKey()} key.
     * Update only if the revision of the event is newer than value in that trigger key.
     *
     * @param revision Event revision.
     * @return Update condition.
     */
    static CompoundCondition triggerKeyCondition(long revision) {
        return or(
                notExists(zonesChangeTriggerKey()),
                value(zonesChangeTriggerKey()).lt(ByteUtils.longToBytes(revision))
        );
    }

    /**
     * Updates data nodes value for a zone and set {@code revision} to {@link DistributionZonesUtil#zonesChangeTriggerKey()}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @param logicalTopologyBytes Logical topology.
     * @return Update command for the meta storage.
     */
    static Update updateDataNodesAndTriggerKey(int zoneId, long revision, byte[] logicalTopologyBytes) {
        return ops(
                put(zoneDataNodesKey(zoneId), logicalTopologyBytes),
                put(zonesChangeTriggerKey(), ByteUtils.longToBytes(revision))
        ).yield(true);
    }

    /**
     * Sets {@code revision} to {@link DistributionZonesUtil#zonesChangeTriggerKey()}.
     *
     * @param revision Revision of the event.
     * @return Update command for the meta storage.
     */
    static Update updateTriggerKey(long revision) {
        return ops(put(zonesChangeTriggerKey(), ByteUtils.longToBytes(revision))).yield(true);
    }

    /**
     * Deletes data nodes value for a zone and set {@code revision} to {@link DistributionZonesUtil#zonesChangeTriggerKey()}.
     *
     * @param zoneId Distribution zone id
     * @param revision Revision of the event.
     * @return Update command for the meta storage.
     */
    static Update deleteDataNodesKeyAndUpdateTriggerKey(int zoneId, long revision) {
        return ops(
                remove(zoneDataNodesKey(zoneId)),
                put(zonesChangeTriggerKey(), ByteUtils.longToBytes(revision))
        ).yield(true);
    }
}
