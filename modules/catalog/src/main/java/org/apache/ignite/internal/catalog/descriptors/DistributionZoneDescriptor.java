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

package org.apache.ignite.internal.catalog.descriptors;

import org.apache.ignite.internal.tostring.S;

/**
 * Distribution zone descriptor base class.
 */
public class DistributionZoneDescriptor extends ObjectDescriptor {
    private static final long serialVersionUID = 1093607327002694066L;

    /** Amount of zone partitions. */
    private final int partitions;

    /** Amount of zone replicas. */
    private final int replicas;

    /** Data nodes auto adjust timeout. */
    private final int dataNodesAutoAdjust;

    /** Data nodes auto adjust scale up timeout. */
    private final int dataNodesAutoAdjustScaleUp;

    /** Data nodes auto adjust scale down timeout. */
    private final int dataNodesAutoAdjustScaleDown;


    /**
     * Constructs a distribution zone descriptor.
     *
     * @param id Id of the distribution zone.
     * @param name Name of the zone.
     * @param partitions Amount of partitions in distributions zone.
     * @param replicas Amount of partition replicas.
     * @param dataNodesAutoAdjust Data nodes auto adjust timeout.
     * @param dataNodesAutoAdjustScaleUp Data nodes auto adjust scale up timeout.
     * @param dataNodesAutoAdjustScaleDown Data nodes auto adjust scale down timeout.
     */
    public DistributionZoneDescriptor(
            int id,
            String name,
            int partitions,
            int replicas,
            int dataNodesAutoAdjust,
            int dataNodesAutoAdjustScaleUp,
            int dataNodesAutoAdjustScaleDown
    ) {
        super(id, Type.ZONE, name);

        this.partitions = partitions;
        this.replicas = replicas;
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
    }

    /**
     * Returns amount of zone partitions.
     */
    public int partitions() {
        return partitions;
    }

    /**
     * Return amount of zone replicas.
     */
    public int replicas() {
        return replicas;
    }

    /**
     * Gets timeout in seconds between node added or node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust timeout.
     */
    public int dataNodesAutoAdjust() {
        return dataNodesAutoAdjust;
    }

    /**
     * Gets timeout in seconds between node added topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust scale up timeout.
     */
    public int dataNodesAutoAdjustScaleUp() {
        return dataNodesAutoAdjustScaleUp;
    }

    /**
     * Gets timeout in seconds between node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust scale down timeout.
     */
    public int dataNodesAutoAdjustScaleDown() {
        return dataNodesAutoAdjustScaleDown;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
