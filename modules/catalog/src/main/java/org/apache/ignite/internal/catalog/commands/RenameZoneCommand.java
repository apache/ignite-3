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

import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.zoneOrThrow;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.DistributionZoneExistsValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that renames a zone with specified name.
 */
public class RenameZoneCommand extends AbstractZoneCommand {
    /** Returns builder to create a command to rename zone. */
    public static RenameZoneCommandBuilder builder() {
        return new RenameZoneCommand.Builder();
    }

    private final boolean ifExists;

    private final String newZoneName;

    /**
     * Constructor.
     *
     * @param zoneName Name of the zone.
     * @param newZoneName New name of the zone.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private RenameZoneCommand(String zoneName, boolean ifExists, String newZoneName) throws CatalogValidationException {
        super(zoneName);

        this.ifExists = ifExists;
        this.newZoneName = newZoneName;

        validate();
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogZoneDescriptor zone = zoneOrThrow(catalog, zoneName);

        if (catalog.zone(newZoneName) != null) {
            throw new DistributionZoneExistsValidationException(format("Distribution zone with name '{}' already exists", newZoneName));
        }

        CatalogZoneDescriptor descriptor = new CatalogZoneDescriptor(
                zone.id(),
                newZoneName,
                zone.partitions(),
                zone.replicas(),
                zone.dataNodesAutoAdjust(),
                zone.dataNodesAutoAdjustScaleUp(),
                zone.dataNodesAutoAdjustScaleDown(),
                zone.filter(),
                zone.storageProfiles()
        );

        return List.of(new AlterZoneEntry(descriptor));
    }

    private void validate() {
        validateIdentifier(newZoneName, "New zone name");
    }

    /**
     * Implementation of {@link RenameZoneCommandBuilder}.
     */
    private static class Builder implements RenameZoneCommandBuilder {
        private String zoneName;
        private boolean ifExists;
        private String newZoneName;

        @Override
        public RenameZoneCommandBuilder zoneName(String zoneName) {
            this.zoneName = zoneName;

            return this;
        }

        @Override
        public RenameZoneCommandBuilder ifExists(boolean ifExists) {
            this.ifExists = ifExists;

            return this;
        }

        @Override
        public RenameZoneCommandBuilder newZoneName(String newZoneName) {
            this.newZoneName = newZoneName;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new RenameZoneCommand(zoneName, ifExists, newZoneName);
        }
    }
}
