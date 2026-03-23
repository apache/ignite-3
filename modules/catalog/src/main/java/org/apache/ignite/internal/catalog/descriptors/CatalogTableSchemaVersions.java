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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.IgniteUtils;
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

        private TableVersion assignColumnIds(IdGenerator idGenerator, Object2IntMap<String> knownColumns) {
            if (columns.isEmpty()) {
                return this;
            }

            List<CatalogTableColumnDescriptor> newColumns = new ArrayList<>(columns.size());
            Set<String> columnsExistingInCurrentVersion = IgniteUtils.newHashSet(columns.size());
            for (CatalogTableColumnDescriptor column : columns) {
                columnsExistingInCurrentVersion.add(column.name());

                int newId = knownColumns.computeIfAbsent(column.name(), k -> idGenerator.nextId());

                newColumns.add(column.clone(newId));
            }

            // Cleanup ids for non existing columns as new column may be created with the same name,
            // but they must be assigned with new id.
            knownColumns.keySet().removeIf(name -> !columnsExistingInCurrentVersion.contains(name));

            return new TableVersion(newColumns);
        }
    }

    private final int base;
    private final int nextColumnId;
    private final TableVersion[] versions;

    /**
     * Constructor.
     *
     * @param version Array of table versions.
     */
    public CatalogTableSchemaVersions(TableVersion version) {
        this(
                CatalogTableDescriptor.INITIAL_TABLE_VERSION,
                version.columns.size(),
                version.assignColumnIds(new IdGenerator(0), new Object2IntOpenHashMap<>())
        );
    }

    CatalogTableSchemaVersions(int base, int nextColumnId, TableVersion... versions) {
        validateColumnIdsAreAssigned(nextColumnId, versions);

        this.base = base;
        this.nextColumnId = nextColumnId;
        this.versions = versions;
    }

    int nextColumnId() {
        return nextColumnId;
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
     * Returns latest known table version.
     */
    public List<CatalogTableColumnDescriptor> latestVersionColumns() {
        return versions[versions.length - 1].columns();
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
    public CatalogTableSchemaVersions append(TableVersion tableVersion) {
        TableVersion latestVersion = versions[versions.length - 1];

        Object2IntMap<String> knownColumns = new Object2IntOpenHashMap<>();
        for (CatalogTableColumnDescriptor column : latestVersion.columns) {
            assert column.id() != CatalogTableColumnDescriptor.ID_IS_NOT_ASSIGNED 
                    : "latest existing TableVersion contains column with unassigned id";

            knownColumns.put(column.name(), column.id());
        }

        IdGenerator idGenerator = new IdGenerator(nextColumnId);
        tableVersion = tableVersion.assignColumnIds(idGenerator, knownColumns);

        return new CatalogTableSchemaVersions(base, idGenerator.nextId, ArrayUtils.concat(versions, tableVersion));
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_TABLE_SCHEMA_VERSIONS.id();
    }

    static class IdGenerator {
        private int nextId;

        IdGenerator(int initialId) {
            this.nextId = initialId;
        }

        int nextId() {
            return nextId++;
        }
    }

    static TableVersion[] assignColumnIds(IdGenerator idGenerator, TableVersion[] versions) {
        Object2IntMap<String> knownColumns = new Object2IntOpenHashMap<>();
        TableVersion[] newVersions = new TableVersion[versions.length];

        for (int i = 0; i < versions.length; i++) {
            newVersions[i] = versions[i].assignColumnIds(idGenerator, knownColumns);
        }

        return newVersions;
    }

    private static void validateColumnIdsAreAssigned(int nextColumnId, TableVersion[] versions) {
        if (!IgniteUtils.assertionsEnabled()) {
            return;
        }

        for (TableVersion version : versions) {
            int lastSeenColumnId = CatalogTableColumnDescriptor.ID_IS_NOT_ASSIGNED;

            for (CatalogTableColumnDescriptor column : version.columns) {
                assert column.id() != CatalogTableColumnDescriptor.ID_IS_NOT_ASSIGNED : "One or more column doesn't have an id assigned";
                assert column.id() > lastSeenColumnId : "Column ids must be increasing: " + version.columns;
                assert column.id() < nextColumnId : "Column ids of existing columns must be less than nextColumnId: " + version.columns;

                lastSeenColumnId = column.id();
            }
        }
    }
}
