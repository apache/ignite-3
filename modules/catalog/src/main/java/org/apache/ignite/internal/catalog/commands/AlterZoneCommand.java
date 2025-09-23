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
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateField;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validatePartition;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateZoneFilter;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.zone;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * A command that changes the particular zone.
 */
public class AlterZoneCommand extends AbstractZoneCommand {
    private static final IgniteLogger LOG = Loggers.forClass(AlterZoneCommand.class);

    public static AlterZoneCommandBuilder builder() {
        return new Builder();
    }

    private final boolean ifExists;

    private final @Nullable Integer partitions;

    private final @Nullable Integer replicas;

    private final @Nullable Integer quorumSize;

    private final @Nullable Integer dataNodesAutoAdjustScaleUp;

    private final @Nullable Integer dataNodesAutoAdjustScaleDown;

    private final @Nullable String filter;

    private final @Nullable List<StorageProfileParams> storageProfileParams;

    /**
     * Constructor.
     *
     * @param zoneName Name of the zone.
     * @param ifExists Flag indicating whether the {@code IF EXISTS} was specified.
     * @param partitions Number of partitions.
     * @param replicas Number of replicas.
     * @param quorumSize Quorum size.
     * @param dataNodesAutoAdjustScaleUp Timeout in seconds between node added topology event itself and data nodes switch.
     * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch.
     * @param filter Nodes filter.
     * @param storageProfileParams Storage profiles params.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private AlterZoneCommand(
            String zoneName,
            boolean ifExists,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer quorumSize,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable List<StorageProfileParams> storageProfileParams
    ) throws CatalogValidationException {
        super(zoneName);

        this.ifExists = ifExists;
        this.partitions = partitions;
        this.replicas = replicas;
        this.quorumSize = quorumSize;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.storageProfileParams = storageProfileParams;

        validate();
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public List<UpdateEntry> get(UpdateContext updateContext) {
        Catalog catalog = updateContext.catalog();

        CatalogZoneDescriptor zone = zone(catalog, zoneName, !ifExists);
        if (zone == null) {
            return List.of();
        }

        CatalogZoneDescriptor descriptor = fromParamsAndPreviousValue(zone);

        return List.of(new AlterZoneEntry(descriptor));
    }

    private CatalogZoneDescriptor fromParamsAndPreviousValue(CatalogZoneDescriptor previous) {
        @Nullable Integer scaleUp = null;
        @Nullable Integer scaleDown = null;

        if (dataNodesAutoAdjustScaleUp != null || dataNodesAutoAdjustScaleDown != null) {
            scaleUp = dataNodesAutoAdjustScaleUp;
            scaleDown = dataNodesAutoAdjustScaleDown;
        }

        CatalogStorageProfilesDescriptor storageProfiles = storageProfileParams != null
                ? fromParams(storageProfileParams) : previous.storageProfiles();

        int replicas = requireNonNullElse(this.replicas, previous.replicas());
        int quorumSize = adjustQuorumSize(replicas, previous.quorumSize());

        return new CatalogZoneDescriptor(
                previous.id(),
                previous.name(),
                requireNonNullElse(partitions, previous.partitions()),
                replicas,
                quorumSize,
                requireNonNullElse(scaleUp, previous.dataNodesAutoAdjustScaleUp()),
                requireNonNullElse(scaleDown, previous.dataNodesAutoAdjustScaleDown()),
                requireNonNullElse(filter, previous.filter()),
                storageProfiles,
                previous.consistencyMode()
        );
    }

    /**
     * Adjusts quorum size so it is always consistent with the replicas count.
     *
     * @param replicas Desired replicas count.
     * @param previousQuorumSize Previous quorum size.
     * @return Adjusted quorum size.
     */
    private int adjustQuorumSize(int replicas, int previousQuorumSize) {
        int quorumSize = requireNonNullElse(this.quorumSize, previousQuorumSize);

        int minQuorum = min(replicas, 2);
        int maxQuorum = max(minQuorum, (int) (round(replicas / 2.0)));

        if (quorumSize > maxQuorum) {
            if (this.quorumSize != null) {
                throw new CatalogValidationException(
                        "Quorum size exceeds the maximum quorum value: [quorum size={}, min={}, max={}, replicas count={}].",
                        quorumSize, minQuorum, maxQuorum, replicas
                );
            }
            LOG.info("Quorum size adjusted from {} to {} because is exceeds the maximum quorum value.", quorumSize, maxQuorum);
            return maxQuorum;
        } else if (quorumSize < minQuorum) {
            if (this.quorumSize != null) {
                throw new CatalogValidationException(
                        "Quorum size is less than the minimum quorum value: [quorum size={}, min={}, max={}, replicas count={}].",
                        quorumSize, minQuorum, maxQuorum, replicas
                );
            }
            LOG.info("Quorum size adjusted from {} to {} because it is less than the minimum quorum value.", quorumSize, minQuorum);
            return minQuorum;
        }
        return quorumSize;
    }

    private void validate() {
        validatePartition(partitions);
        validateField(replicas, 1, null, "Invalid number of replicas");
        validateField(quorumSize, 1, null, "Invalid quorum size");
        validateField(dataNodesAutoAdjustScaleUp, 0, null, "Invalid data nodes auto adjust scale up");
        validateField(dataNodesAutoAdjustScaleDown, 0, null, "Invalid data nodes auto adjust scale down");

        validateZoneFilter(filter);
    }

    /**
     * Implementation of {@link AlterZoneCommandBuilder}.
     */
    private static class Builder implements AlterZoneCommandBuilder {
        private String zoneName;

        private boolean ifExists;

        private @Nullable Integer partitions;

        private @Nullable Integer replicas;

        private @Nullable Integer quorumSize;

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
        public AlterZoneCommandBuilder ifExists(boolean ifExists) {
            this.ifExists = ifExists;

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
        public AlterZoneCommandBuilder quorumSize(Integer quorumSize) {
            this.quorumSize = quorumSize;

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
                    ifExists,
                    partitions,
                    replicas,
                    quorumSize,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    storageProfileParams
            );
        }
    }
}
