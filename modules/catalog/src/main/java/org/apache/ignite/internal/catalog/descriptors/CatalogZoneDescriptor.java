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

import static java.lang.Math.min;
import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;

import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;

/**
 * Distribution zone descriptor base class.
 */
public class CatalogZoneDescriptor extends CatalogObjectDescriptor implements MarshallableEntry {
    /** Amount of zone partitions. */
    private final int partitions;

    /** Amount of zone replicas. */
    private final int replicas;

    /** Quorum size. */
    private final int quorumSize;

    /**
     * Data nodes auto adjust timeout. Deprecated field, do not use it anymore.  Use {@link #dataNodesAutoAdjustScaleUp} and
     * {@link #dataNodesAutoAdjustScaleDown} instead. The field is left for backward compatibility.
     */
    private final int dataNodesAutoAdjust;

    /** Data nodes auto adjust scale up timeout. */
    private final int dataNodesAutoAdjustScaleUp;

    /** Data nodes auto adjust scale down timeout. */
    private final int dataNodesAutoAdjustScaleDown;

    /** Nodes filer. */
    private final String filter;

    /** Storage profiles descriptor. */
    private final CatalogStorageProfilesDescriptor storageProfiles;

    /**
     * Specifies the consistency mode of the zone, determining how the system balances data consistency and availability.
     */
    private final ConsistencyMode consistencyMode;

    /**
     * Returns {@code true} if zone upgrade will lead to assignments recalculation.
     */
    public static boolean updateRequiresAssignmentsRecalculation(CatalogZoneDescriptor oldDescriptor, CatalogZoneDescriptor newDescriptor) {
        if (oldDescriptor.updateTimestamp().equals(newDescriptor.updateTimestamp())) {
            return false;
        }

        return oldDescriptor.partitions != newDescriptor.partitions
                || oldDescriptor.replicas != newDescriptor.replicas
                || oldDescriptor.quorumSize != newDescriptor.quorumSize
                || !oldDescriptor.filter.equals(newDescriptor.filter)
                || !oldDescriptor.storageProfiles.profiles().equals(newDescriptor.storageProfiles.profiles())
                || oldDescriptor.consistencyMode != newDescriptor.consistencyMode;
    }

    /**
     * Constructs a distribution zone descriptor.
     *
     * @param id Id of the distribution zone.
     * @param name Name of the zone.
     * @param partitions Count of partitions in distributions zone.
     * @param replicas Count of partition replicas.
     * @param quorumSize Quorum size.
     * @param dataNodesAutoAdjust Data nodes auto adjust timeout.
     * @param dataNodesAutoAdjustScaleUp Data nodes auto adjust scale up timeout.
     * @param dataNodesAutoAdjustScaleDown Data nodes auto adjust scale down timeout.
     * @param filter Nodes filter.
     * @param storageProfiles Storage profiles descriptor.
     * @param consistencyMode Consistency mode of the zone.
     */
    @Deprecated
    public CatalogZoneDescriptor(
            int id,
            String name,
            int partitions,
            int replicas,
            int quorumSize,
            int dataNodesAutoAdjust,
            int dataNodesAutoAdjustScaleUp,
            int dataNodesAutoAdjustScaleDown,
            String filter,
            CatalogStorageProfilesDescriptor storageProfiles,
            ConsistencyMode consistencyMode
    ) {
        this(id, name, partitions, replicas, quorumSize, dataNodesAutoAdjust, dataNodesAutoAdjustScaleUp, dataNodesAutoAdjustScaleDown,
                filter, storageProfiles, INITIAL_TIMESTAMP, consistencyMode);
    }

    /**
     * Constructs a distribution zone descriptor.
     *
     * @param id Id of the distribution zone.
     * @param name Name of the zone.
     * @param partitions Count of partitions in distributions zone.
     * @param replicas Count of partition replicas.
     * @param quorumSize Quorum size.
     * @param dataNodesAutoAdjustScaleUp Data nodes auto adjust scale up timeout.
     * @param dataNodesAutoAdjustScaleDown Data nodes auto adjust scale down timeout.
     * @param filter Nodes filter.
     * @param storageProfiles Storage profiles descriptor.
     * @param consistencyMode Consistency mode of the zone.
     */
    public CatalogZoneDescriptor(
            int id,
            String name,
            int partitions,
            int replicas,
            int quorumSize,
            int dataNodesAutoAdjustScaleUp,
            int dataNodesAutoAdjustScaleDown,
            String filter,
            CatalogStorageProfilesDescriptor storageProfiles,
            ConsistencyMode consistencyMode
    ) {
        this(id, name, partitions, replicas, quorumSize, 0, dataNodesAutoAdjustScaleUp, dataNodesAutoAdjustScaleDown,
                filter, storageProfiles, INITIAL_TIMESTAMP, consistencyMode);
    }

    /**
     * Constructs a distribution zone descriptor.
     *
     * @param id Id of the distribution zone.
     * @param name Name of the zone.
     * @param partitions Count of partitions in distributions zone.
     * @param replicas Count of partition replicas.
     * @param dataNodesAutoAdjust Data nodes auto adjust timeout.
     * @param dataNodesAutoAdjustScaleUp Data nodes auto adjust scale up timeout.
     * @param dataNodesAutoAdjustScaleDown Data nodes auto adjust scale down timeout.
     * @param filter Nodes filter.
     * @param timestamp Timestamp of the update of the descriptor.
     */
    @Deprecated
    CatalogZoneDescriptor(
            int id,
            String name,
            int partitions,
            int replicas,
            int quorumSize,
            int dataNodesAutoAdjust,
            int dataNodesAutoAdjustScaleUp,
            int dataNodesAutoAdjustScaleDown,
            String filter,
            CatalogStorageProfilesDescriptor storageProfiles,
            HybridTimestamp timestamp,
            ConsistencyMode consistencyMode
    ) {
        super(id, Type.ZONE, name, timestamp);

        this.partitions = partitions;
        this.replicas = replicas;
        this.quorumSize = quorumSize;
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.storageProfiles = storageProfiles;
        this.consistencyMode = consistencyMode;
    }

    /**
     * Constructs a distribution zone descriptor.
     *
     * @param id Id of the distribution zone.
     * @param name Name of the zone.
     * @param partitions Count of partitions in distributions zone.
     * @param replicas Count of partition replicas.
     * @param dataNodesAutoAdjustScaleUp Data nodes auto adjust scale up timeout.
     * @param dataNodesAutoAdjustScaleDown Data nodes auto adjust scale down timeout.
     * @param filter Nodes filter.
     * @param timestamp Timestamp of the update of the descriptor.
     */
    CatalogZoneDescriptor(
            int id,
            String name,
            int partitions,
            int replicas,
            int quorumSize,
            int dataNodesAutoAdjustScaleUp,
            int dataNodesAutoAdjustScaleDown,
            String filter,
            CatalogStorageProfilesDescriptor storageProfiles,
            HybridTimestamp timestamp,
            ConsistencyMode consistencyMode
    ) {
        super(id, Type.ZONE, name, timestamp);

        this.partitions = partitions;
        this.replicas = replicas;
        this.quorumSize = quorumSize;
        this.dataNodesAutoAdjust = 0;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.storageProfiles = storageProfiles;
        this.consistencyMode = consistencyMode;
    }

    /**
     * Returns count of zone partitions.
     */
    public int partitions() {
        return partitions;
    }

    /**
     * Return count of zone replicas.
     */
    public int replicas() {
        return replicas;
    }

    /**
     * Return quorum size. Quorum is the minimal subset of replicas in the consensus group that is required for it to be fully operational
     * and maintain the data consistency, in the case of Raft it is the majority of voting members.
     */
    public int quorumSize() {
        return quorumSize;
    }

    /**
     * Return consensus group size. Consensus group is a subset of replicas of a partition that maintains the data consistency in the
     * replication group, in the case of Raft it is the set of voting members. Derived from the quorum size.
     */
    public int consensusGroupSize() {
        return min(quorumSize * 2 - 1, replicas);
    }

    /**
     * Gets timeout in seconds between node added or node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust timeout.
     */
    @Deprecated
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

    /**
     * Returns the nodes filter.
     */
    public String filter() {
        return filter;
    }

    /**
     * Specifies the consistency mode of the zone, determining how the system balances data consistency and availability.
     */
    public ConsistencyMode consistencyMode() {
        return consistencyMode;
    }

    /**
     * Returns the storage profiles descriptor.
     */
    public CatalogStorageProfilesDescriptor storageProfiles() {
        return storageProfiles;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_ZONE.id();
    }

    @Override
    public String toString() {
        return S.toString(CatalogZoneDescriptor.class, this, super.toString());
    }
}
