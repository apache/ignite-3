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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateField;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateZoneDataNodesAutoAdjustParametersCompatibility;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateZoneFilter;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STORAGE_ENGINE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.DistributionZoneExistsValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.jetbrains.annotations.Nullable;

/**
 * A command that creates a new zone.
 */
public class CreateZoneCommand extends AbstractZoneCommand {
    /** Returns builder to create a command to create a zone with specified name. */
    public static CreateZoneCommandBuilder builder() {
        return new Builder();
    }

    private final @Nullable Integer partitions;

    private final @Nullable Integer replicas;

    private final @Nullable Integer dataNodesAutoAdjust;

    private final @Nullable Integer dataNodesAutoAdjustScaleUp;

    private final @Nullable Integer dataNodesAutoAdjustScaleDown;

    private final @Nullable String filter;

    private final @Nullable DataStorageParams dataStorageParams;

    /**
     * Constructor.
     *
     * @param zoneName Name of the zone.
     * @param partitions Number of partitions.
     * @param replicas Number of replicas.
     * @param dataNodesAutoAdjust Timeout in seconds between node added or node left topology event itself and data nodes switch.
     * @param dataNodesAutoAdjustScaleUp Timeout in seconds between node added topology event itself and data nodes switch.
     * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch.
     * @param filter Nodes filter.
     * @param dataStorageParams Data storage params.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private CreateZoneCommand(
            String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjust,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable DataStorageParams dataStorageParams
    ) throws CatalogValidationException {
        super(zoneName);

        this.partitions = partitions;
        this.replicas = replicas;
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.dataStorageParams = dataStorageParams;

        validate();
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        if (catalog.zone(zoneName) != null) {
            throw new DistributionZoneExistsValidationException("Distribution zone already exists [zoneName=" + zoneName + ']');
        }

        CatalogZoneDescriptor zoneDesc = descriptor(catalog.objectIdGenState());

        return List.of(
                new NewZoneEntry(zoneDesc),
                new ObjectIdGenUpdateEntry(1)
        );
    }

    private CatalogZoneDescriptor descriptor(int objectId) {
        DataStorageParams dataStorageParams0 = dataStorageParams != null
                ? dataStorageParams
                : DataStorageParams.builder().engine(DEFAULT_STORAGE_ENGINE).dataRegion(DEFAULT_DATA_REGION).build();

        CatalogZoneDescriptor zone = new CatalogZoneDescriptor(
                objectId,
                zoneName,
                Objects.requireNonNullElse(partitions, DEFAULT_PARTITION_COUNT),
                Objects.requireNonNullElse(replicas, DEFAULT_REPLICA_COUNT),
                Objects.requireNonNullElse(dataNodesAutoAdjust, INFINITE_TIMER_VALUE),
                Objects.requireNonNullElse(
                        dataNodesAutoAdjustScaleUp,
                        dataNodesAutoAdjust != null ? INFINITE_TIMER_VALUE : IMMEDIATE_TIMER_VALUE
                ),
                Objects.requireNonNullElse(dataNodesAutoAdjustScaleDown, INFINITE_TIMER_VALUE),
                Objects.requireNonNullElse(filter, DEFAULT_FILTER),
                fromParams(dataStorageParams0)
        );

        return zone;
    }

    private void validate() {
        validateField(partitions, 1, MAX_PARTITION_COUNT, "Invalid number of partitions");
        validateField(replicas, 1, null, "Invalid number of replicas");
        validateField(dataNodesAutoAdjust, 0, null, "Invalid data nodes auto adjust");
        validateField(dataNodesAutoAdjustScaleUp, 0, null, "Invalid data nodes auto adjust scale up");
        validateField(dataNodesAutoAdjustScaleDown, 0, null, "Invalid data nodes auto adjust scale down");

        validateZoneDataNodesAutoAdjustParametersCompatibility(
                dataNodesAutoAdjust,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown
        );

        validateZoneFilter(filter);
    }

    /**
     * Implementation of {@link CreateZoneCommandBuilder}.
     */
    private static class Builder implements CreateZoneCommandBuilder {
        private String zoneName;

        private @Nullable Integer partitions;

        private @Nullable Integer replicas;

        private @Nullable Integer dataNodesAutoAdjust;

        private @Nullable Integer dataNodesAutoAdjustScaleUp;

        private @Nullable Integer dataNodesAutoAdjustScaleDown;

        private @Nullable String filter;

        private @Nullable DataStorageParams dataStorageParams;

        @Override
        public CreateZoneCommandBuilder zoneName(String zoneName) {
            this.zoneName = zoneName;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder partitions(Integer partitions) {
            this.partitions = partitions;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder replicas(Integer replicas) {
            this.replicas = replicas;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder dataNodesAutoAdjust(Integer adjust) {
            dataNodesAutoAdjust = adjust;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder dataNodesAutoAdjustScaleUp(Integer adjust) {
            dataNodesAutoAdjustScaleUp = adjust;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder dataNodesAutoAdjustScaleDown(Integer adjust) {
            dataNodesAutoAdjustScaleDown = adjust;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder filter(String filter) {
            this.filter = filter;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder dataStorageParams(DataStorageParams params) {
            this.dataStorageParams = params;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new CreateZoneCommand(
                    zoneName,
                    partitions,
                    replicas,
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    dataStorageParams);
        }
    }
}
