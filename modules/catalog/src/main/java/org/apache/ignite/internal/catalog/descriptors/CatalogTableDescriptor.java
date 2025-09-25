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

import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import it.unimi.dsi.fastutil.ints.AbstractInt2ObjectMap.BasicEntry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Table descriptor.
 */
public class CatalogTableDescriptor extends CatalogObjectDescriptor implements MarshallableEntry, CatalogColumnContainer {
    public static final int INITIAL_TABLE_VERSION = 1;

    private final int zoneId;

    private final int schemaId;

    private final int pkIndexId;

    @IgniteToStringExclude
    private final CatalogTableSchemaVersions schemaVersions;

    @IgniteToStringInclude
    private final List<CatalogTableColumnDescriptor> columns;

    @IgniteToStringInclude
    private final List<String> primaryKeyColumns;

    @IgniteToStringInclude
    private final List<String> colocationColumns;

    @IgniteToStringExclude
    private final Map<String, Int2ObjectMap.Entry<CatalogTableColumnDescriptor>> columnsMap;

    private final String storageProfile;

    /**
     * Internal constructor.
     *
     * @param id Table ID.
     * @param pkIndexId Primary key index ID.
     * @param name Table name.
     * @param zoneId Distribution zone ID.
     * @param columns Table column descriptors.
     * @param pkCols Primary key column names.
     * @param storageProfile Storage profile.
     * @param timestamp Token of the update of the descriptor.
     */
    private CatalogTableDescriptor(
            int id,
            int schemaId,
            int pkIndexId,
            String name,
            int zoneId,
            List<CatalogTableColumnDescriptor> columns,
            List<String> pkCols,
            @Nullable List<String> colocationCols,
            CatalogTableSchemaVersions schemaVersions,
            String storageProfile,
            HybridTimestamp timestamp
    ) {
        super(id, Type.TABLE, name, timestamp);

        this.schemaId = schemaId;
        this.pkIndexId = pkIndexId;
        this.zoneId = zoneId;
        this.columns = Objects.requireNonNull(columns, "No columns defined.");
        this.primaryKeyColumns = Objects.requireNonNull(pkCols, "No primary key columns.");

        Map<String, Int2ObjectMap.Entry<CatalogTableColumnDescriptor>> columnMap = IgniteUtils.newHashMap(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            CatalogTableColumnDescriptor column = columns.get(i);
            columnMap.put(column.name(), new BasicEntry<>(i, column));
        }

        this.columnsMap = columnMap;
        this.colocationColumns = Objects.requireNonNullElse(colocationCols, pkCols);
        this.schemaVersions =  Objects.requireNonNull(schemaVersions, "No catalog schema versions.");
        this.storageProfile = Objects.requireNonNull(storageProfile, "No storage profile.");

        TableVersion latestTableVersion = Objects.requireNonNull(schemaVersions.get(schemaVersions.latestVersion()));
        if (!Objects.equals(latestTableVersion.columns(), columns)) {
            throw new IllegalArgumentException(format(
                    "Latest schema version columns do not match descriptor definition columns. Schema columns: {}, table columns: {}.",
                    latestTableVersion.columns(),
                    columns
            ));
        }
    }

    /**
     * Creates a builder of copy of this table descriptor prepopulated with parameters of this descriptor.
     *
     * @return new Builder.
     */
    public Builder copyBuilder() {
        return new Builder()
                .id(id())
                .name(name())
                .timestamp(updateTimestamp())
                .zoneId(zoneId())
                .schemaId(schemaId())
                .primaryKeyIndexId(primaryKeyIndexId())
                .schemaVersions(schemaVersions)
                .columns(columns)
                .primaryKeyColumns(primaryKeyColumns())
                .colocationColumns(colocationColumns())
                .storageProfile(storageProfile());
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns an identifier of a schema this table descriptor belongs to.
     */
    public int schemaId() {
        return schemaId;
    }

    /**
     * Returns versions of this table descriptor.
     */
    public CatalogTableSchemaVersions schemaVersions() {
        return schemaVersions;
    }

    /**
     * Returns an identifier of a distribution zone this table descriptor belongs to.
     */
    public int zoneId() {
        return zoneId;
    }

    /**
     * Returns a identifier of the primary key index.
     */
    public int primaryKeyIndexId() {
        return pkIndexId;
    }

    /**
     * Returns a version of this table descriptor.
     */
    public int tableVersion() {
        return schemaVersions.latestVersion();
    }

    /**
     * Returns a list primary key column names.
     */
    public List<String> primaryKeyColumns() {
        return primaryKeyColumns;
    }

    /**
     * Returns a list colocation key column names.
     */
    public List<String> colocationColumns() {
        return colocationColumns;
    }

    /** {@inheritDoc} */
    @Override
    public List<CatalogTableColumnDescriptor> columns() {
        return columns;
    }

    /** Returns a column descriptor for column with given name, {@code null} if absent. */
    public @Nullable CatalogTableColumnDescriptor column(String name) {
        Int2ObjectMap.Entry<CatalogTableColumnDescriptor> column = columnsMap.get(name);
        if (column != null) {
            return column.getValue();
        } else {
            return null;
        }
    }

    /**
     * Returns an index of a column with the given name, or {@code -1} if such column does not exist.
     */
    public int columnIndex(String name) {
        Int2ObjectMap.Entry<CatalogTableColumnDescriptor> column = columnsMap.get(name);
        if (column != null) {
            return column.getIntKey();
        } else {
            return -1;
        }
    }

    /**
     * Returns {@code true} if this the given column is a part of the primary key.
     */
    public boolean isPrimaryKeyColumn(String name) {
        return primaryKeyColumns.contains(name);
    }

    /**
     * Returns {@code true} if this the given column is a part of colocation key.
     */
    public boolean isColocationColumn(String name) {
        return colocationColumns.contains(name);
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_TABLE.id();
    }

    @Override
    public String toString() {
        return S.toString(CatalogTableDescriptor.class, this, super.toString());
    }

    /**
     * Returns a name of a storage profile.
     */
    public String storageProfile() {
        return storageProfile;
    }

    /**
     * {@code CatalogTableDescriptor} builder static inner class.
     */
    public static final class Builder {
        private int id;
        private String name;
        private int zoneId;
        private int schemaId;
        private int pkIndexId;
        private CatalogTableSchemaVersions schemaVersions;
        private List<CatalogTableColumnDescriptor> columns;
        private List<String> primaryKeyColumns;
        @Nullable private List<String> colocationColumns;
        private String storageProfile;
        private HybridTimestamp timestamp = INITIAL_TIMESTAMP;
        private int tableVersion = 0;

        /**
         * Sets the {@code id} and returns a reference to this Builder enabling method chaining.
         *
         * @param id the {@code id} to set
         * @return a reference to this Builder
         */
        public Builder id(int id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the {@code name} and returns a reference to this Builder enabling method chaining.
         *
         * @param name the {@code name} to set
         * @return a reference to this Builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the {@code timestamp} and returns a reference to this Builder enabling method chaining.
         *
         * @param timestamp the {@code timestamp} to set
         * @return a reference to this Builder
         */
        public Builder timestamp(HybridTimestamp timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * Sets the {@code zoneId} and returns a reference to this Builder enabling method chaining.
         *
         * @param zoneId the {@code zoneId} to set
         * @return a reference to this Builder
         */
        public Builder zoneId(int zoneId) {
            this.zoneId = zoneId;
            return this;
        }

        /**
         * Sets the {@code schemaId} and returns a reference to this Builder enabling method chaining.
         *
         * @param schemaId the {@code schemaId} to set
         * @return a reference to this Builder
         */
        public Builder schemaId(int schemaId) {
            this.schemaId = schemaId;
            return this;
        }

        /**
         * Sets the {@code primaryKeyIndexId} and returns a reference to this Builder enabling method chaining.
         *
         * @param primaryKeyIndexId the {@code primaryKeyIndexId} to set
         * @return a reference to this Builder
         */
        public Builder primaryKeyIndexId(int primaryKeyIndexId) {
            this.pkIndexId = primaryKeyIndexId;
            return this;
        }

        /**
         * Sets the {@code schemaVersions} and returns a reference to this Builder enabling method chaining.
         *
         * @param schemaVersions the {@code schemaVersions} to set
         * @return a reference to this Builder
         */
        public Builder schemaVersions(CatalogTableSchemaVersions schemaVersions) {
            this.schemaVersions = schemaVersions;
            return this;
        }

        /**
         * Sets the {@code columns} and returns a reference to this Builder enabling method chaining.
         *
         * @param columns the {@code columns} to set
         * @return a reference to this Builder
         */
        public Builder columns(List<CatalogTableColumnDescriptor> columns) {
            this.columns = columns;
            return this;
        }

        /**
         * Sets the {@code primaryKeyColumns} and returns a reference to this Builder enabling method chaining.
         *
         * @param primaryKeyColumns the {@code primaryKeyColumns} to set
         * @return a reference to this Builder
         */
        public Builder primaryKeyColumns(List<String> primaryKeyColumns) {
            this.primaryKeyColumns = primaryKeyColumns;
            return this;
        }

        /**
         * Sets the {@code colocationColumns} and returns a reference to this Builder enabling method chaining.
         *
         * @param colocationColumns the {@code colocationColumns} to set
         * @return a reference to this Builder
         */
        public Builder colocationColumns(@Nullable List<String> colocationColumns) {
            this.colocationColumns = colocationColumns;
            return this;
        }

        /**
         * Sets the {@code storageProfile} and returns a reference to this Builder enabling method chaining.
         *
         * @param storageProfile the {@code storageProfile} to set
         * @return a reference to this Builder
         */
        public Builder storageProfile(String storageProfile) {
            this.storageProfile = storageProfile;
            return this;
        }

        /**
         * Sets the {@code tableVersion} and returns a reference to this Builder enabling method chaining.
         *
         * @param tableVersion the {@code tableVersion} to set.
         * @return a reference to this Builder.
         */
        public Builder tableVersion(int tableVersion) {
            this.tableVersion = tableVersion;
            return this;
        }

        /**
         * Returns a {@code CatalogTableDescriptor} built from the parameters previously set.
         *
         * @return a {@code CatalogTableDescriptor} built with parameters of this {@code CatalogTableDescriptor.Builder}
         */
        public CatalogTableDescriptor build() {
            Objects.requireNonNull(columns, "No columns defined.");
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("No columns defined.");
            }

            if (schemaVersions == null) {
                // in case we are creating a new table from scratch, if schema version is not defined we make a default one
                schemaVersions = new CatalogTableSchemaVersions(new TableVersion(columns));
            }

            if (tableVersion == 0) {
                tableVersion = schemaVersions.latestVersion();
            } else if (tableVersion < schemaVersions.latestVersion()) {
                throw new IllegalArgumentException(format(
                        "Table version {} should not be less than a previous version {}.",
                        tableVersion,
                        schemaVersions.latestVersion()
                ));
            }

            // TODO: https://issues.apache.org/jira/browse/IGNITE-26501
            CatalogTableSchemaVersions newSchemaVersions = tableVersion == schemaVersions.latestVersion()
                    ? schemaVersions
                    : schemaVersions.append(new TableVersion(columns), tableVersion);

            return new CatalogTableDescriptor(
                    id,
                    schemaId,
                    pkIndexId,
                    name,
                    zoneId,
                    columns,
                    primaryKeyColumns,
                    colocationColumns,
                    newSchemaVersions,
                    storageProfile,
                    timestamp
            );
        }
    }
}
