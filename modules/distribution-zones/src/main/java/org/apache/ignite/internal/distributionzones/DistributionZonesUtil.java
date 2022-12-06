package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.metastorage.client.CompoundCondition.or;
import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.value;

import org.apache.ignite.internal.metastorage.client.CompoundCondition;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;

/**
 * Util class for Distribution Zones flow.
 */
public class DistributionZonesUtil {
    /** Key prefix for zone's data nodes. */
    private static final String DISTRIBUTION_ZONE_DATA_NODES_PREFIX = "distributionZone.dataNodes";

    /** ByteArray representation of {@link DistributionZonesUtil#DISTRIBUTION_ZONE_DATA_NODES_PREFIX}. */
    public static ByteArray zoneDataNodesKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_PREFIX + zoneId);
    }

    /**
     * The key, needed for processing the event about zones' update was triggered only once.
     */
    public static ByteArray zonesChangeTriggerKey() {
        return new ByteArray("distributionZones.change.trigger");
    }

    /**
     * Condition for updating {@link DistributionZonesUtil#zonesChangeTriggerKey()} key
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
}
