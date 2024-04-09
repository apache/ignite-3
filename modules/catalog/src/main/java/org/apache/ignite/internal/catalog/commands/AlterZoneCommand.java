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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.zoneOrThrow;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.jetbrains.annotations.Nullable;

/**
 * A command that changes the particular zone.
 */
public class AlterZoneCommand extends AbstractZoneCommand {
    public static AlterZoneCommandBuilder builder() {
        return new Builder();
    }

    private final @Nullable Integer partitions;

    private final @Nullable Integer replicas;

    private final @Nullable Integer dataNodesAutoAdjust;

    private final @Nullable Integer dataNodesAutoAdjustScaleUp;

    private final @Nullable Integer dataNodesAutoAdjustScaleDown;

    private final @Nullable String filter;

    private final @Nullable List<StorageProfileParams> storageProfileParams;

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
     * @param storageProfileParams Storage profiles params.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private AlterZoneCommand(
            String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjust,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable List<StorageProfileParams> storageProfileParams
    ) throws CatalogValidationException {
        super(zoneName);

        this.partitions = partitions;
        this.replicas = replicas;
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.storageProfileParams = storageProfileParams;

        validate();
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogZoneDescriptor zone = zoneOrThrow(catalog, zoneName);

        CatalogZoneDescriptor descriptor = fromParamsAndPreviousValue(zone);

        return List.of(new AlterZoneEntry(descriptor));
    }

    private CatalogZoneDescriptor fromParamsAndPreviousValue(CatalogZoneDescriptor previous) {
        @Nullable Integer autoAdjust = null;
        @Nullable Integer scaleUp = null;
        @Nullable Integer scaleDown = null;

        if (dataNodesAutoAdjust != null) {
            autoAdjust = dataNodesAutoAdjust;
            scaleUp = INFINITE_TIMER_VALUE;
            scaleDown = INFINITE_TIMER_VALUE;
        } else if (dataNodesAutoAdjustScaleUp != null || dataNodesAutoAdjustScaleDown != null) {
            autoAdjust = INFINITE_TIMER_VALUE;
            scaleUp = dataNodesAutoAdjustScaleUp;
            scaleDown = dataNodesAutoAdjustScaleDown;
        }

        CatalogStorageProfilesDescriptor storageProfiles = storageProfileParams != null
                ? fromParams(storageProfileParams) : previous.storageProfiles();

        return new CatalogZoneDescriptor(
                previous.id(),
                previous.name(),
                Objects.requireNonNullElse(partitions, previous.partitions()),
                Objects.requireNonNullElse(replicas, previous.replicas()),
                Objects.requireNonNullElse(autoAdjust, previous.dataNodesAutoAdjust()),
                Objects.requireNonNullElse(scaleUp, previous.dataNodesAutoAdjustScaleUp()),
                Objects.requireNonNullElse(scaleDown, previous.dataNodesAutoAdjustScaleDown()),
                Objects.requireNonNullElse(filter, previous.filter()),
                storageProfiles
        );
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
     * Implementation of {@link AlterZoneCommandBuilder}.
     */
    private static class Builder implements AlterZoneCommandBuilder {
        private String zoneName;

        private @Nullable Integer partitions;

        private @Nullable Integer replicas;

        private @Nullable Integer dataNodesAutoAdjust;

        private @Nullable Integer dataNodesAutoAdjustScaleUp;

        private @Nullable Integer dataNodesAutoAdjustScaleDown;

        private @Nullable String filter;

        private @Nullable List<StorageProfileParams> storageProfileParams;

        @Override
        public AlterZoneCommandBuilder zoneName(String zoneName) {
            this.zoneName = zoneName;

            return this;
        }

        @Override
        public AlterZoneCommandBuilder partitions(Integer partitions) {
            this.partitions = partitions;

            return this;
        }

        @Override
        public AlterZoneCommandBuilder replicas(Integer replicas) {
            this.replicas = replicas;

            return this;
        }

        @Override
        public AlterZoneCommandBuilder dataNodesAutoAdjust(Integer adjust) {
            dataNodesAutoAdjust = adjust;

            return this;
        }

        @Override
        public AlterZoneCommandBuilder dataNodesAutoAdjustScaleUp(Integer adjust) {
            dataNodesAutoAdjustScaleUp = adjust;

            return this;
        }

        @Override
        public AlterZoneCommandBuilder dataNodesAutoAdjustScaleDown(Integer adjust) {
            dataNodesAutoAdjustScaleDown = adjust;

            return this;
        }

        @Override
        public AlterZoneCommandBuilder filter(String filter) {
            this.filter = filter;

            return this;
        }

        @Override
        public AlterZoneCommandBuilder storageProfilesParams(@Nullable List<StorageProfileParams> params) {
            this.storageProfileParams = params;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new AlterZoneCommand(
                    zoneName,
                    partitions,
                    replicas,
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    storageProfileParams
            );
        }
    }
}
