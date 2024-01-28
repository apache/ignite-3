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

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;

import java.io.IOException;
import org.apache.ignite.internal.catalog.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Distribution zone descriptor base class.
 */
public class CatalogZoneDescriptor extends CatalogObjectDescriptor {
    public static CatalogObjectSerializer<CatalogZoneDescriptor> SERIALIZER = new ZoneDescriptorSerializer();

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

    /** Nodes filer. */
    private final String filter;

    /** Data storage descriptor. */
    private final CatalogDataStorageDescriptor dataStorage;

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
     * @param dataStorage Data storage descriptor.
     */
    public CatalogZoneDescriptor(
            int id,
            String name,
            int partitions,
            int replicas,
            int dataNodesAutoAdjust,
            int dataNodesAutoAdjustScaleUp,
            int dataNodesAutoAdjustScaleDown,
            String filter,
            CatalogDataStorageDescriptor dataStorage
    ) {
        this(id, name, partitions, replicas, dataNodesAutoAdjust, dataNodesAutoAdjustScaleUp, dataNodesAutoAdjustScaleDown,
                filter, dataStorage, INITIAL_CAUSALITY_TOKEN);
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
     * @param dataStorage Data storage descriptor.
     * @param causalityToken Token of the update of the descriptor.
     */
    private CatalogZoneDescriptor(
            int id,
            String name,
            int partitions,
            int replicas,
            int dataNodesAutoAdjust,
            int dataNodesAutoAdjustScaleUp,
            int dataNodesAutoAdjustScaleDown,
            String filter,
            CatalogDataStorageDescriptor dataStorage,
            long causalityToken
    ) {
        super(id, Type.ZONE, name, causalityToken);

        this.partitions = partitions;
        this.replicas = replicas;
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.dataStorage = dataStorage;
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

    /**
     * Returns the nodes filter.
     */
    public String filter() {
        return filter;
    }

    /**
     * Returns the data storage descriptor.
     */
    // TODO: IGNITE-19719 Must be storage engine specific
    public CatalogDataStorageDescriptor dataStorage() {
        return dataStorage;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link CatalogZoneDescriptor}.
     */
    private static class ZoneDescriptorSerializer implements CatalogObjectSerializer<CatalogZoneDescriptor> {
        @Override
        public CatalogZoneDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            int id = input.readInt();
            String name = input.readUTF();
            long updateToken = input.readLong();

            CatalogDataStorageDescriptor dataStorageDescriptor = CatalogDataStorageDescriptor.SERIALIZER.readFrom(version, input);

            int partitions = input.readInt();
            int replicas = input.readInt();
            int dataNodesAutoAdjust = input.readInt();
            int dataNodesAutoAdjustScaleUp = input.readInt();
            int dataNodesAutoAdjustScaleDown = input.readInt();
            String filter = input.readUTF();

            return new CatalogZoneDescriptor(
                    id,
                    name,
                    partitions,
                    replicas,
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    dataStorageDescriptor,
                    updateToken
            );
        }

        @Override
        public void writeTo(CatalogZoneDescriptor descriptor, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeLong(descriptor.updateToken());
            CatalogDataStorageDescriptor.SERIALIZER.writeTo(descriptor.dataStorage(), version, output);

            output.writeInt(descriptor.partitions());
            output.writeInt(descriptor.replicas());
            output.writeInt(descriptor.dataNodesAutoAdjust());
            output.writeInt(descriptor.dataNodesAutoAdjustScaleUp());
            output.writeInt(descriptor.dataNodesAutoAdjustScaleDown());
            output.writeUTF(descriptor.filter());
        }
    }
}
