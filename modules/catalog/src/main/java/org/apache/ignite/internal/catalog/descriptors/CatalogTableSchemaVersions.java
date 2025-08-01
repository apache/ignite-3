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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Class that holds a list of table version descriptors.
 */
public class CatalogTableSchemaVersions implements MarshallableEntry {
    /**
     * Descriptor of a single table version.
     */
    public static class TableVersion implements MarshallableEntry {
        private final List<CatalogTableColumnDescriptor> columns;

        public TableVersion(List<CatalogTableColumnDescriptor> columns) {
            this.columns = columns;
        }

        public List<CatalogTableColumnDescriptor> columns() {
            return Collections.unmodifiableList(columns);
        }

        @Override
        public int typeId() {
            return MarshallableEntryType.DESCRIPTOR_TABLE_VERSION.id();
        }
    }

    private final int base;
    private final TableVersion[] versions;

    /**
     * Constructor.
     *
     * @param versions Array of table versions.
     */
    public CatalogTableSchemaVersions(TableVersion... versions) {
        this(CatalogTableDescriptor.INITIAL_TABLE_VERSION, versions);
    }

    CatalogTableSchemaVersions(int base, TableVersion... versions) {
        this.base = base;
        this.versions = versions;
    }

    /**
     * Returns earliest known table version.
     */
    public int earliestVersion() {
        return base;
    }

    /**
     * Returns latest known table version.
     */
    public int latestVersion() {
        return base + versions.length - 1;
    }

    /**
     * Returns all known table versions.
     */
    TableVersion[] versions() {
        return versions;
    }

    /**
     * Returns an existing table version, or {@code null} if it's not found.
     */
    public @Nullable TableVersion get(int version) {
        if (version < base || version >= base + versions.length) {
            return null;
        }

        return versions[version - base];
    }

    /**
     * Creates a new instance of {@link CatalogTableSchemaVersions} with one new version appended.
     */
    public CatalogTableSchemaVersions append(TableVersion tableVersion, int version) {
        assert version == latestVersion() + 1;

        return new CatalogTableSchemaVersions(base, ArrayUtils.concat(versions, tableVersion));
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_TABLE_SCHEMA_VERSIONS.id();
    }
}
