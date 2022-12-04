package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.metastorage.client.CompoundCondition.or;
import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.value;

import org.apache.ignite.internal.metastorage.client.CompoundCondition;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;

public class DistributionZonesUtil {
    /**  */
    public static final String DISTRIBUTION_ZONE_DATA_NODES_PREFIX = "distributionZone.dataNodes";

    public static CompoundCondition triggerKeyCondition(long revision, ByteArray zonesChangeTriggerKey) {
        return or(
                notExists(zonesChangeTriggerKey),
                value(zonesChangeTriggerKey).lt(ByteUtils.longToBytes(revision))
        );
    }

    /**
     *
     *
     */
    public static ByteArray zonesChangeTriggerKey() {
        return new ByteArray("distributionZones.change.trigger");
    }

    public static ByteArray zoneDataNodesKey(int zoneId) {
        return new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_PREFIX + zoneId);
    }
}
