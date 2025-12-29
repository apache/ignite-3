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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateConsistencyMode;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateField;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateStorageProfiles;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateZoneFilter;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultQuorumSize;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.duplicateDistributionZoneNameCatalogValidationException;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.STRONG_CONSISTENCY;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.PartitionCountCalculationParameters;
import org.apache.ignite.internal.catalog.PartitionCountProvider;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
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

    private final boolean ifNotExists;

    private final @Nullable Integer partitions;

    private final @Nullable Integer replicas;

    private final @Nullable Integer quorumSize;

    private final @Nullable Integer dataNodesAutoAdjustScaleUp;

    private final @Nullable Integer dataNodesAutoAdjustScaleDown;

    private final @Nullable String filter;

    private final List<StorageProfileParams> storageProfileParams;

    private final @Nullable ConsistencyMode consistencyMode;

    /**
     * Constructor.
     *
     * @param zoneName Name of the zone.
     * @param ifNotExists Flag indicating whether the {@code IF NOT EXISTS} was specified.
     * @param partitions Number of partitions.
     * @param replicas Number of replicas.
     * @param quorumSize Quorum size.
     * @param dataNodesAutoAdjustScaleUp Timeout in seconds between node added topology event itself and data nodes switch.
     * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch.
     * @param filter Nodes filter.
     * @param storageProfileParams Storage profile params.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private CreateZoneCommand(
            String zoneName,
            boolean ifNotExists,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer quorumSize,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            List<StorageProfileParams> storageProfileParams,
            @Nullable ConsistencyMode consistencyMode
    ) throws CatalogValidationException {
        super(zoneName);
        this.ifNotExists = ifNotExists;
        this.partitions = partitions;
        this.replicas = replicas;
        this.quorumSize = quorumSize;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.storageProfileParams = storageProfileParams;
        this.consistencyMode = consistencyMode;

        validate();
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @Override
    public List<UpdateEntry> get(UpdateContext updateContext) {
        Catalog catalog = updateContext.catalog();
        if (catalog.zone(zoneName) != null) {
            if (ifNotExists) {
                return List.of();
            }

            throw duplicateDistributionZoneNameCatalogValidationException(zoneName);
        }

        CatalogZoneDescriptor zoneDesc = descriptor(updateContext.partitionCountProvider(), catalog.objectIdGenState());

        return List.of(
                new NewZoneEntry(zoneDesc),
                new ObjectIdGenUpdateEntry(1)
        );
    }

    private CatalogZoneDescriptor descriptor(PartitionCountProvider partitionCountProvider, int objectId) {
        String filter = requireNonNullElse(this.filter, DEFAULT_FILTER);
        int replicas = requireNonNullElse(this.replicas, DEFAULT_REPLICA_COUNT);
        CatalogStorageProfilesDescriptor storageProfilesDescriptor = fromParams(storageProfileParams);
        List<String> storageProfiles = storageProfilesDescriptor.profiles()
                .stream()
                .map(CatalogStorageProfileDescriptor::storageProfile)
                .collect(toList());

        return new CatalogZoneDescriptor(
                objectId,
                zoneName,
                requireNonNullElse(partitions, partitionCountProvider.calculate(
                        PartitionCountCalculationParameters.builder()
                                .dataNodesFilter(filter)
                                .storageProfiles(storageProfiles)
                                .replicaFactor(replicas)
                                .build()
                )),
                replicas,
                requireNonNullElse(quorumSize, defaultQuorumSize(replicas)),
                requireNonNullElse(
                        dataNodesAutoAdjustScaleUp,
                        IMMEDIATE_TIMER_VALUE
                ),
                requireNonNullElse(dataNodesAutoAdjustScaleDown, INFINITE_TIMER_VALUE),
                filter,
                storageProfilesDescriptor,
                requireNonNullElse(consistencyMode, STRONG_CONSISTENCY)
        );
    }

    private void validate() {
        validateField(partitions, 1, MAX_PARTITION_COUNT, "Invalid number of partitions");
        validateField(replicas, 1, null, "Invalid number of replicas");
        validateField(quorumSize, 1, null, "Invalid quorum size");

        int replicas = requireNonNullElse(this.replicas, DEFAULT_REPLICA_COUNT);
        int quorumSize = requireNonNullElse(this.quorumSize, defaultQuorumSize(replicas));
        validateReplicasAndQuorumCompatibility(replicas, quorumSize);

        validateField(dataNodesAutoAdjustScaleUp, 0, null, "Invalid data nodes auto adjust scale up");
        validateField(dataNodesAutoAdjustScaleDown, 0, null, "Invalid data nodes auto adjust scale down");

        validateZoneFilter(filter);

        validateConsistencyMode(consistencyMode);

        validateStorageProfiles(storageProfileParams);
    }

    /**
     * Validates replicas count and quorum size compatibility.
     *
     * @param replicas Number of replicas.
     * @param quorumSize Quorum size.
     */
    private void validateReplicasAndQuorumCompatibility(int replicas, int quorumSize) {
        int minQuorum = min(replicas, 2);
        int maxQuorum = max(minQuorum, (int) (round(replicas / 2.0)));

        if (quorumSize < minQuorum || quorumSize > maxQuorum) {
            throw new CatalogValidationException(
                    "{}: [quorum size={}, min={}, max={}, replicas count={}].",
                    getErrPrefix(), quorumSize, minQuorum, maxQuorum, replicas
            );
        }
    }

    private String getErrPrefix() {
        if (this.quorumSize != null) {
            if (this.replicas != null) {
                return "Specified quorum size doesn't fit into the specified replicas count";
            } else {
                return "Specified quorum size doesn't fit into the default replicas count";
            }
        } else {
            if (this.replicas != null) {
                return "Current quorum size doesn't fit into the specified replicas count";
            } else {
                // Should never happen - this means that the default zone parameters are incompatible
                return "Default quorum size doesn't fit into the default replicas count";
            }
        }
    }

    /**
     * Implementation of {@link CreateZoneCommandBuilder}.
     */
    private static class Builder implements CreateZoneCommandBuilder {
        private String zoneName;

        private boolean ifNotExists;

        private @Nullable Integer partitions;

        private @Nullable Integer replicas;

        private @Nullable Integer quorumSize;

        private @Nullable Integer dataNodesAutoAdjustScaleUp;

        private @Nullable Integer dataNodesAutoAdjustScaleDown;

        private @Nullable String filter;

        private @Nullable ConsistencyMode consistencyMode;

        private List<StorageProfileParams> storageProfileParams;

        @Override
        public CreateZoneCommandBuilder zoneName(String zoneName) {
            this.zoneName = zoneName;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder ifNotExists(boolean ifNotExists) {
            this.ifNotExists = ifNotExists;

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
        public CreateZoneCommandBuilder quorumSize(Integer quorumSize) {
            this.quorumSize = quorumSize;

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
        public CreateZoneCommandBuilder storageProfilesParams(List<StorageProfileParams> params) {
            this.storageProfileParams = params;

            return this;
        }

        @Override
        public CreateZoneCommandBuilder consistencyModeParams(@Nullable ConsistencyMode params) {
            this.consistencyMode = params;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new CreateZoneCommand(
                    zoneName,
                    ifNotExists,
                    partitions,
                    replicas,
                    quorumSize,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    storageProfileParams,
                    consistencyMode
            );
        }
    }
}
