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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.zoneOrThrow;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.DistributionZoneCantBeDroppedValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.DropZoneEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that drops a zone with specified name.
 */
public class DropZoneCommand extends AbstractZoneCommand {
    /** Returns builder to create a command to drop zone with specified name. */
    public static DropZoneCommandBuilder builder() {
        return new Builder();
    }

    /**
     * Constructor.
     *
     * @param zoneName Name of the zone.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private DropZoneCommand(String zoneName) throws CatalogValidationException {
        super(zoneName);
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogZoneDescriptor zone = zoneOrThrow(catalog, zoneName);

        if (zone.id() == catalog.defaultZone().id()) {
            throw new DistributionZoneCantBeDroppedValidationException("Default distribution zone can't be dropped: zoneName={}", zoneName);
        }

        catalog.schemas().stream()
                .flatMap(s -> Arrays.stream(s.tables()))
                .filter(t -> t.zoneId() == zone.id())
                .findAny()
                .ifPresent(t -> {
                    throw new DistributionZoneCantBeDroppedValidationException("Distribution zone '{}' is assigned to the table '{}'",
                            zone.name(), t.name());
                });

        return List.of(new DropZoneEntry(zone.id()));
    }

    /**
     * Implementation of {@link DropZoneCommandBuilder}.
     */
    private static class Builder implements DropZoneCommandBuilder {
        private String zoneName;

        @Override
        public DropZoneCommandBuilder zoneName(String zoneName) {
            this.zoneName = zoneName;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new DropZoneCommand(zoneName);
        }
    }
}
