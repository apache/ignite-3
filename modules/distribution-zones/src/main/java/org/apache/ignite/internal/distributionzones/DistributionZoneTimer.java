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

import static java.util.Collections.unmodifiableSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.nodeNames;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * Timer representation for distribution zones.
 */
public class DistributionZoneTimer {
    /** Default timer. */
    public static final DistributionZoneTimer DEFAULT_TIMER = new DistributionZoneTimer(HybridTimestamp.MIN_VALUE, 0, Set.of());

    private final HybridTimestamp createTimestamp;

    private final int timeToWaitInSeconds;

    private final Set<NodeWithAttributes> nodes;

    /**
     * Constructor.
     *
     * @param createTimestamp Timestamp when the timer was created.
     * @param timeToWaitInSeconds Time to wait in seconds. {@link CatalogUtils#INFINITE_TIMER_VALUE} is acceptable here.
     * @param nodes Nodes that will be affected by the timer. This is a set of added nodes (for scale up timer) or removed node
     *     (for scale down timer).
     */
    public DistributionZoneTimer(
            HybridTimestamp createTimestamp,
            int timeToWaitInSeconds,
            Set<NodeWithAttributes> nodes
    ) {
        this.createTimestamp = createTimestamp;
        this.timeToWaitInSeconds = timeToWaitInSeconds;
        this.nodes = nodes;
    }

    /**
     * Timestamp when the timer was created.
     *
     * @return Timestamp.
     */
    public HybridTimestamp createTimestamp() {
        return createTimestamp;
    }

    /**
     * Time to wait in seconds.
     *
     * @return Time to wait in seconds. {@link CatalogUtils#INFINITE_TIMER_VALUE} is acceptable here.
     */
    public int timeToWaitInSeconds() {
        return timeToWaitInSeconds;
    }

    /**
     * Nodes that will be affected by the timer. This is a set of added nodes (for scale up timer) or removed node (for scale down timer).
     *
     * @return Set of nodes.
     */
    public Set<NodeWithAttributes> nodes() {
        return unmodifiableSet(nodes);
    }

    /**
     * Timestamp when the timer should be triggered.
     *
     * @return Timestamp to trigger.
     */
    public HybridTimestamp timeToTrigger() {
        if (timeToWaitInSeconds == INFINITE_TIMER_VALUE) {
            return HybridTimestamp.MAX_VALUE;
        }

        return createTimestamp.addPhysicalTime(timeToWaitInSeconds * 1000L);
    }

    /**
     * Create a new timer with the same timestamp and nodes, but with a new time to wait.
     *
     * @param newTimeToWaitInSeconds New time to wait in seconds.
     * @return New timer.
     */
    public DistributionZoneTimer modifyTimeToWait(int newTimeToWaitInSeconds) {
        if (equals(DEFAULT_TIMER)) {
            return DEFAULT_TIMER;
        } else {
            return new DistributionZoneTimer(createTimestamp, newTimeToWaitInSeconds, nodes);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributionZoneTimer that = (DistributionZoneTimer) o;
        return timeToWaitInSeconds == that.timeToWaitInSeconds && Objects.equals(createTimestamp, that.createTimestamp)
                && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(createTimestamp, timeToWaitInSeconds, nodes);
    }

    @Override
    public String toString() {
        String timeToWaitStr = (timeToWaitInSeconds == INFINITE_TIMER_VALUE ? "[infinite]" : String.valueOf(timeToWaitInSeconds));

        return equals(DEFAULT_TIMER) ? "[empty]" : "[timestamp=" + createTimestamp
                + ", timeToWaitInSeconds=" + timeToWaitStr
                + ", nodes=" + nodeNames(nodes) + ']';
    }

    /**
     * Serializer.
     */
    public static class DistributionZoneTimerSerializer extends VersionedSerializer<DistributionZoneTimer> {
        /** Serializer instance. */
        private static final DistributionZoneTimerSerializer INSTANCE = new DistributionZoneTimerSerializer();

        @Override
        protected void writeExternalData(DistributionZoneTimer object, IgniteDataOutput out) throws IOException {
            // Using new TreeSet() because the order of elements is important: the serialized representation is compared during
            // MetaStorageManager#invoke().
            out.writeVarInt(object.createTimestamp.longValue());
            out.writeVarInt(object.timeToWaitInSeconds);
            out.writeCollection(new TreeSet<>(object.nodes), NodeWithAttributesSerializer.INSTANCE::writeExternal);
        }

        @Override
        protected DistributionZoneTimer readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
            HybridTimestamp timestamp = HybridTimestamp.hybridTimestamp(in.readVarInt());
            int timeToWaitInSeconds = in.readVarIntAsInt();
            Set<NodeWithAttributes> nodes = in.readCollection(
                    unused -> new TreeSet<>(),
                    NodeWithAttributesSerializer.INSTANCE::readExternal
            );

            return new DistributionZoneTimer(timestamp, timeToWaitInSeconds, nodes);
        }

        public static byte[] serialize(DistributionZoneTimer timer) {
            return VersionedSerialization.toBytes(timer, INSTANCE);
        }

        public static DistributionZoneTimer deserialize(byte[] bytes) {
            return VersionedSerialization.fromBytes(bytes, INSTANCE);
        }
    }
}
