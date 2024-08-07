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
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;

import it.unimi.dsi.fastutil.ints.AbstractInt2ObjectMap.BasicEntry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * Table descriptor.
 */
public class CatalogTableDescriptor extends CatalogObjectDescriptor {
    public static final CatalogObjectSerializer<CatalogTableDescriptor> SERIALIZER = new TableDescriptorSerializer();

    public static final int INITIAL_TABLE_VERSION = 1;

    private final int zoneId;

    private final int schemaId;

    private final int pkIndexId;

    @IgniteToStringExclude
    private final CatalogTableSchemaVersions schemaVersions;

    private final List<CatalogTableColumnDescriptor> columns;
    @IgniteToStringInclude
    private final List<String> primaryKeyColumns;
    @IgniteToStringInclude
    private final List<String> colocationColumns;

    @IgniteToStringExclude
    private Map<String, Int2ObjectMap.Entry<CatalogTableColumnDescriptor>> columnsMap;

    private long creationToken;

    private String storageProfile;

    /**
     * Constructor for new table.
     *
     * @param id Table id.
     * @param pkIndexId Primary key index id.
     * @param name Table name.
     * @param zoneId Distribution zone ID.
     * @param columns Table column descriptors.
     * @param pkCols Primary key column names.
     * @param storageProfile Storage profile.
     */
    public CatalogTableDescriptor(
            int id,
            int schemaId,
            int pkIndexId,
            String name,
            int zoneId,
            List<CatalogTableColumnDescriptor> columns,
            List<String> pkCols,
            @Nullable List<String> colocationCols,
            String storageProfile
    ) {
        this(id, schemaId, pkIndexId, name, zoneId, columns, pkCols, colocationCols,
                new CatalogTableSchemaVersions(new TableVersion(columns)),
                storageProfile, INITIAL_CAUSALITY_TOKEN, INITIAL_CAUSALITY_TOKEN);
    }

    /**
     * Internal constructor.
     *
     * @param id Table id.
     * @param pkIndexId Primary key index id.
     * @param name Table name.
     * @param zoneId Distribution zone ID.
     * @param columns Table column descriptors.
     * @param pkCols Primary key column names.
     * @param storageProfile Storage profile.
     * @param causalityToken Token of the update of the descriptor.
     * @param creationToken Token of the creation of the table descriptor.
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
            long causalityToken,
            long creationToken
    ) {
        super(id, Type.TABLE, name, causalityToken);

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
        this.creationToken = creationToken;
    }

    /**
     * Creates new table descriptor, using existing one as a template.
     */
    public CatalogTableDescriptor newDescriptor(
            String name,
            int tableVersion,
            List<CatalogTableColumnDescriptor> columns,
            long causalityToken,
            String storageProfile
    ) {
        CatalogTableSchemaVersions newSchemaVersions = tableVersion == schemaVersions.latestVersion()
                ? schemaVersions
                : schemaVersions.append(new TableVersion(columns), tableVersion);

        return new CatalogTableDescriptor(
                id(), schemaId, pkIndexId, name, zoneId, columns, primaryKeyColumns, colocationColumns,
                newSchemaVersions,
                storageProfile, causalityToken, creationToken
        );
    }

    /**
     * Returns column descriptor for column with given name.
     */
    public CatalogTableColumnDescriptor columnDescriptor(String columnName) {
        Entry<Integer, CatalogTableColumnDescriptor> column = columnsMap.get(columnName);
        if (column != null) {
            return column.getValue();
        } else {
            return null;
        }
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

    /**
     * Returns a list column descriptors for the table.
     */
    public List<CatalogTableColumnDescriptor> columns() {
        return columns;
    }

    /**
     * Returns a column descriptor for column with given name.
     */
    public CatalogTableColumnDescriptor column(String name) {
        Entry<Integer, CatalogTableColumnDescriptor> column = columnsMap.get(name);
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
        Entry<Integer, CatalogTableColumnDescriptor> column = columnsMap.get(name);
        if (column != null) {
            return column.getKey();
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
     * Returns {@code true} if this the given column is a part of collocation key.
     */
    public boolean isColocationColumn(String name) {
        return colocationColumns.contains(name);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(CatalogTableDescriptor.class, this, super.toString());
    }

    /**
     * Returns a creation token.
     */
    public long creationToken() {
        return creationToken;
    }

    /**
     * Returns a name of a storage profile.
     */
    public String storageProfile() {
        return storageProfile;
    }

    /** {@inheritDoc} */
    @Override
    public void updateToken(long updateToken) {
        super.updateToken(updateToken);

        this.creationToken = this.creationToken == INITIAL_CAUSALITY_TOKEN ? updateToken : this.creationToken;
    }

    /**
     * Serializer for {@link CatalogTableDescriptor}.
     */
    private static class TableDescriptorSerializer implements CatalogObjectSerializer<CatalogTableDescriptor> {
        @Override
        public CatalogTableDescriptor readFrom(IgniteDataInput input) throws IOException {
            int id = input.readInt();
            String name = input.readUTF();
            long updateToken = input.readLong();

            CatalogTableSchemaVersions schemaVersions = CatalogTableSchemaVersions.SERIALIZER.readFrom(input);
            List<CatalogTableColumnDescriptor> columns = readList(CatalogTableColumnDescriptor.SERIALIZER, input);
            String storageProfile = input.readUTF();

            int schemaId = input.readInt();
            int pkIndexId = input.readInt();
            int zoneId = input.readInt();

            int pkKeysLen = input.readInt();
            int[] pkColumnIndexes = input.readIntArray(pkKeysLen);
            List<String> primaryKeyColumns = new ArrayList<>(pkColumnIndexes.length);

            for (int idx : pkColumnIndexes) {
                primaryKeyColumns.add(columns.get(idx).name());
            }

            int colocationColumnsLen = input.readInt();

            List<String> colocationColumns;

            if (colocationColumnsLen == -1) {
                colocationColumns = primaryKeyColumns;
            } else {
                colocationColumns = new ArrayList<>(colocationColumnsLen);

                int[] colocationColumnIdxs = input.readIntArray(colocationColumnsLen);

                for (int idx : colocationColumnIdxs) {
                    colocationColumns.add(columns.get(idx).name());
                }
            }

            long creationToken = input.readLong();

            return new CatalogTableDescriptor(
                    id,
                    schemaId,
                    pkIndexId,
                    name,
                    zoneId,
                    columns,
                    primaryKeyColumns,
                    colocationColumns,
                    schemaVersions,
                    storageProfile,
                    updateToken,
                    creationToken
            );
        }

        @Override
        public void writeTo(CatalogTableDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeLong(descriptor.updateToken());
            CatalogTableSchemaVersions.SERIALIZER.writeTo(descriptor.schemaVersions(), output);
            writeList(descriptor.columns(), CatalogTableColumnDescriptor.SERIALIZER, output);
            output.writeUTF(descriptor.storageProfile());

            output.writeInt(descriptor.schemaId());
            output.writeInt(descriptor.primaryKeyIndexId());
            output.writeInt(descriptor.zoneId());

            int[] pkIndexes = resolvePkColumnIndexes(descriptor);

            output.writeInt(pkIndexes.length);
            output.writeIntArray(pkIndexes);

            if (descriptor.colocationColumns() == descriptor.primaryKeyColumns()) {
                output.writeInt(-1);
            } else {
                int[] colocationIndexes = resolveColocationColumnIndexes(pkIndexes, descriptor);

                output.writeInt(colocationIndexes.length);
                output.writeIntArray(colocationIndexes);
            }

            output.writeLong(descriptor.creationToken());
        }

        private static int[] resolveColocationColumnIndexes(int[] pkColumnIndexes, CatalogTableDescriptor descriptor) {
            int[] colocationColumnIndexes = new int[descriptor.colocationColumns().size()];

            for (int idx : pkColumnIndexes) {
                String columnName = descriptor.columns.get(idx).name();

                for (int j = 0; j < descriptor.colocationColumns().size(); j++) {
                    if (descriptor.colocationColumns().get(j).equals(columnName)) {
                        colocationColumnIndexes[j] = idx;

                        break;
                    }
                }
            }

            return colocationColumnIndexes;
        }

        private static int[] resolvePkColumnIndexes(CatalogTableDescriptor descriptor) {
            List<CatalogTableColumnDescriptor> columns = descriptor.columns();
            List<String> pkColumns = descriptor.primaryKeyColumns();

            assert columns.size() >= pkColumns.size();

            int[] pkColumnIndexes = new int[pkColumns.size()];
            int foundCount = 0;

            for (int i = 0; i < columns.size() && foundCount < pkColumnIndexes.length; i++) {
                for (int j = 0; j < pkColumns.size(); j++) {
                    String pkColumn = pkColumns.get(j);

                    if (pkColumn.equals(columns.get(i).name())) {
                        pkColumnIndexes[j] = i;
                        foundCount++;

                        break;
                    }
                }
            }

            assert foundCount == pkColumnIndexes.length;

            return pkColumnIndexes;
        }
    }
}
