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
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readStringList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeStringCollection;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor.CatalogDescriptorBaseSerializer.CatalogDescriptorBase;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.catalog.serialization.CatalogEntrySerializer;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * Table descriptor.
 */
public class CatalogTableDescriptor extends CatalogObjectDescriptor {
    public static CatalogEntrySerializer<CatalogTableDescriptor> SERIALIZER = new TableDescriptorSerializer();

    public static final int INITIAL_TABLE_VERSION = 1;

    private final int zoneId;

    private final int schemaId;

    private final int pkIndexId;

    private final CatalogTableSchemaVersions schemaVersions;

    private final List<CatalogTableColumnDescriptor> columns;
    private final List<String> primaryKeyColumns;
    private final List<String> colocationColumns;

    @IgniteToStringExclude
    private Map<String, CatalogTableColumnDescriptor> columnsMap;

    private long creationToken;

    /**
     * Constructor for new table.
     *
     * @param id Table id.
     * @param pkIndexId Primary key index id.
     * @param name Table name.
     * @param zoneId Distribution zone ID.
     * @param columns Table column descriptors.
     * @param pkCols Primary key column names.
     */
    public CatalogTableDescriptor(
            int id,
            int schemaId,
            int pkIndexId,
            String name,
            int zoneId,
            List<CatalogTableColumnDescriptor> columns,
            List<String> pkCols,
            @Nullable List<String> colocationCols
    ) {
        this(id, schemaId, pkIndexId, name, zoneId, columns, pkCols, colocationCols,
                new CatalogTableSchemaVersions(new TableVersion(columns)), INITIAL_CAUSALITY_TOKEN, INITIAL_CAUSALITY_TOKEN);
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
     * @param causalityToken Token of the update of the descriptor.
     * @param creationToken Token of the creation of the table descriptor.
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
            CatalogTableSchemaVersions schemaVersions,
            long causalityToken,
            long creationToken
    ) {
        super(id, Type.TABLE, name, causalityToken);

        this.schemaId = schemaId;
        this.pkIndexId = pkIndexId;
        this.zoneId = zoneId;
        this.columns = Objects.requireNonNull(columns, "No columns defined.");
        primaryKeyColumns = Objects.requireNonNull(pkCols, "No primary key columns.");
        colocationColumns = colocationCols == null ? pkCols : colocationCols;

        this.columnsMap = columns.stream().collect(Collectors.toMap(CatalogTableColumnDescriptor::name, Function.identity()));

        this.schemaVersions = schemaVersions;

        this.creationToken = creationToken;

        // TODO: IGNITE-19082 Throw proper exceptions.
        assert !columnsMap.isEmpty() : "No columns.";

        assert primaryKeyColumns.stream().noneMatch(c -> Objects.requireNonNull(columnsMap.get(c), c).nullable());
        assert Set.copyOf(primaryKeyColumns).containsAll(colocationColumns);
    }

    /**
     * Creates new table descriptor, using existing one as a template.
     */
    public CatalogTableDescriptor newDescriptor(
            String name,
            int tableVersion,
            List<CatalogTableColumnDescriptor> columns,
            long causalityToken
    ) {
        CatalogTableSchemaVersions newSchemaVersions = tableVersion == schemaVersions.latestVersion()
                ? schemaVersions
                : schemaVersions.append(new TableVersion(columns), tableVersion);

        return new CatalogTableDescriptor(
                id(), schemaId, pkIndexId, name, zoneId, columns, primaryKeyColumns, colocationColumns,
                newSchemaVersions,
                causalityToken, creationToken
        );
    }

    /**
     * Returns column descriptor for column with given name.
     */
    public CatalogTableColumnDescriptor columnDescriptor(String columnName) {
        return columnsMap.get(columnName);
    }

    public int schemaId() {
        return schemaId;
    }

    public CatalogTableSchemaVersions schemaVersions() {
        return schemaVersions;
    }

    public int zoneId() {
        return zoneId;
    }

    public int primaryKeyIndexId() {
        return pkIndexId;
    }

    public int tableVersion() {
        return schemaVersions.latestVersion();
    }

    public List<String> primaryKeyColumns() {
        return primaryKeyColumns;
    }

    public List<String> colocationColumns() {
        return colocationColumns;
    }

    public List<CatalogTableColumnDescriptor> columns() {
        return columns;
    }

    public CatalogTableColumnDescriptor column(String name) {
        return columnsMap.get(name);
    }

    public boolean isPrimaryKeyColumn(String name) {
        return primaryKeyColumns.contains(name);
    }

    public boolean isColocationColumn(String name) {
        return colocationColumns.contains(name);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    public long creationToken() {
        return creationToken;
    }

    @Override
    public void updateToken(long updateToken) {
        super.updateToken(updateToken);

        this.creationToken = this.creationToken == INITIAL_CAUSALITY_TOKEN ? updateToken : this.creationToken;
    }

    /**
     * Serializer for {@link CatalogTableDescriptor}.
     */
    private static class TableDescriptorSerializer implements CatalogEntrySerializer<CatalogTableDescriptor> {
        @Override
        public CatalogTableDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogDescriptorBase header = CatalogObjectDescriptor.SERIALIZER.readFrom(version, input);
            CatalogTableSchemaVersions schemaVersions = CatalogTableSchemaVersions.SERIALIZER.readFrom(version, input);
            List<CatalogTableColumnDescriptor> columns = readList(version, input, CatalogTableColumnDescriptor.SERIALIZER);

            int schemaId = input.readInt();
            int pkIndexId = input.readInt();
            int zoneId = input.readInt();

            List<String> primaryKeyColumns = readStringList(input);
            List<String> colocationColumns = readStringList(input);

            long creationToken = input.readLong();

            return new CatalogTableDescriptor(
                    header.id(),
                    schemaId,
                    pkIndexId,
                    header.name(),
                    zoneId,
                    columns,
                    primaryKeyColumns,
                    colocationColumns,
                    schemaVersions,
                    header.updateToken(),
                    creationToken
            );
        }

        @Override
        public void writeTo(CatalogTableDescriptor descriptor, int version, IgniteDataOutput output) throws IOException {
            CatalogObjectDescriptor.SERIALIZER.writeTo(new CatalogDescriptorBase(descriptor), version, output);
            CatalogTableSchemaVersions.SERIALIZER.writeTo(descriptor.schemaVersions(), version, output);
            writeList(descriptor.columns(), version, CatalogTableColumnDescriptor.SERIALIZER, output);

            output.writeInt(descriptor.schemaId());
            output.writeInt(descriptor.primaryKeyIndexId());
            output.writeInt(descriptor.zoneId());
            writeStringCollection(descriptor.primaryKeyColumns(), output);
            writeStringCollection(descriptor.colocationColumns(), output);
            output.writeLong(descriptor.creationToken());
        }
    }
}
