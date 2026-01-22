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
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Serializers for {@link CatalogTableDescriptor}.
 */
public class CatalogTableDescriptorSerializers {
    /**
     * Serializer for {@link CatalogTableDescriptor}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class TableDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogTableDescriptor> {
        private final CatalogEntrySerializerProvider serializers;

        public TableDescriptorSerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public CatalogTableDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();

            // Read the update token.
            input.readVarInt();

            CatalogObjectSerializer<CatalogTableSchemaVersions> schemaVerSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_SCHEMA_VERSIONS.id());
            CatalogObjectSerializer<CatalogTableColumnDescriptor> tableColumnSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id());

            CatalogTableSchemaVersions schemaVersions = schemaVerSerializer.readFrom(input);
            readList(tableColumnSerializer, input); // column list
            String storageProfile = input.readUTF();

            int schemaId = input.readVarIntAsInt();
            int pkIndexId = input.readVarIntAsInt();
            int zoneId = input.readVarIntAsInt();

            int pkKeysLen = input.readVarIntAsInt();
            int[] pkColumnIndexes = input.readIntArray(pkKeysLen);

            List<CatalogTableColumnDescriptor> columns = schemaVersions.latestVersionColumns();

            IntList primaryKeyColumns = new IntArrayList(pkColumnIndexes.length);
            for (int idx : pkColumnIndexes) {
                primaryKeyColumns.add(columns.get(idx).id());
            }

            int colocationColumnsLen = input.readVarIntAsInt();

            IntList colocationColumns;

            if (colocationColumnsLen == -1) {
                colocationColumns = primaryKeyColumns;
            } else {
                int[] colocationColumnIdxs = input.readIntArray(colocationColumnsLen);
                colocationColumns = resolveColumnIdsByIndexes(columns, colocationColumnIdxs);
            }

            return CatalogTableDescriptor.builder()
                    .id(id)
                    .schemaId(schemaId)
                    .primaryKeyIndexId(pkIndexId)
                    .name(name)
                    .zoneId(zoneId)
                    .primaryKeyColumns(primaryKeyColumns)
                    .colocationColumns(colocationColumns)
                    .schemaVersions(schemaVersions)
                    .storageProfile(storageProfile)
                    // Here we use the initial timestamp because it's old storage.
                    .timestamp(INITIAL_TIMESTAMP)
                    .build();
        }

        @Override
        public void writeTo(CatalogTableDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());

            CatalogTableSchemaVersions schemaVersions = descriptor.schemaVersions();
            CatalogObjectSerializer<CatalogTableColumnDescriptor> tableColumnSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id());

            serializers.get(1, schemaVersions.typeId()).writeTo(schemaVersions, output);
            writeList(descriptor.columns(), tableColumnSerializer, output);
            output.writeUTF(descriptor.storageProfile());

            output.writeVarInt(descriptor.schemaId());
            output.writeVarInt(descriptor.primaryKeyIndexId());
            output.writeVarInt(descriptor.zoneId());

            int[] pkIndexes = resolvePkColumnIndexes(descriptor);

            output.writeVarInt(pkIndexes.length);
            output.writeIntArray(pkIndexes);

            if (descriptor.colocationColumns() == descriptor.primaryKeyColumns()) {
                output.writeVarInt(-1);
            } else {
                int[] colocationIndexes = resolveColocationColumnIndexes(pkIndexes, descriptor);

                output.writeVarInt(colocationIndexes.length);
                output.writeIntArray(colocationIndexes);
            }
        }
    }

    /**
     * Serializer for {@link CatalogTableDescriptor}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    static class TableDescriptorSerializerV2 implements CatalogObjectSerializer<CatalogTableDescriptor> {
        @Override
        public CatalogTableDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateTimestampLong = input.readVarInt();
            HybridTimestamp updateTimestamp = updateTimestampLong == 0 ? MIN_VALUE : hybridTimestamp(updateTimestampLong);

            CatalogTableSchemaVersions schemaVersions = input.readEntry(CatalogTableSchemaVersions.class);
            input.readEntryList(CatalogTableColumnDescriptor.class); // column list

            String storageProfile = input.readUTF();

            int schemaId = input.readVarIntAsInt();
            int pkIndexId = input.readVarIntAsInt();
            int zoneId = input.readVarIntAsInt();

            int pkKeysLen = input.readVarIntAsInt();
            int[] pkColumnIndexes = input.readIntArray(pkKeysLen);

            List<CatalogTableColumnDescriptor> columns = schemaVersions.latestVersionColumns();

            IntList primaryKeyColumns = new IntArrayList(pkColumnIndexes.length);

            for (int idx : pkColumnIndexes) {
                primaryKeyColumns.add(columns.get(idx).id());
            }

            int colocationColumnsLen = input.readVarIntAsInt();

            IntList colocationColumns;

            if (colocationColumnsLen == -1) {
                colocationColumns = primaryKeyColumns;
            } else {
                int[] colocationColumnIdxs = input.readIntArray(colocationColumnsLen);
                colocationColumns = resolveColumnIdsByIndexes(columns, colocationColumnIdxs);
            }

            return CatalogTableDescriptor.builder()
                    .id(id)
                    .schemaId(schemaId)
                    .primaryKeyIndexId(pkIndexId)
                    .name(name)
                    .zoneId(zoneId)
                    .primaryKeyColumns(primaryKeyColumns)
                    .colocationColumns(colocationColumns)
                    .schemaVersions(schemaVersions)
                    .storageProfile(storageProfile)
                    .timestamp(updateTimestamp)
                    .build();
        }

        @Override
        public void writeTo(CatalogTableDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());

            output.writeEntry(descriptor.schemaVersions());
            output.writeEntryList(descriptor.columns());
            output.writeUTF(descriptor.storageProfile());

            output.writeVarInt(descriptor.schemaId());
            output.writeVarInt(descriptor.primaryKeyIndexId());
            output.writeVarInt(descriptor.zoneId());

            int[] pkIndexes = resolvePkColumnIndexes(descriptor);

            output.writeVarInt(pkIndexes.length);
            output.writeIntArray(pkIndexes);

            if (descriptor.colocationColumns() == descriptor.primaryKeyColumns()) {
                output.writeVarInt(-1);
            } else {
                int[] colocationIndexes = resolveColocationColumnIndexes(pkIndexes, descriptor);

                output.writeVarInt(colocationIndexes.length);
                output.writeIntArray(colocationIndexes);
            }
        }
    }

    /**
     * Serializer for {@link CatalogTableDescriptor}.
     */
    @CatalogSerializer(version = 3, since = "3.2.0")
    static class TableDescriptorSerializerV3 implements CatalogObjectSerializer<CatalogTableDescriptor> {
        @Override
        public CatalogTableDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateTimestampLong = input.readVarInt();
            HybridTimestamp updateTimestamp = updateTimestampLong == 0 ? MIN_VALUE : hybridTimestamp(updateTimestampLong);

            CatalogTableSchemaVersions schemaVersions = input.readEntry(CatalogTableSchemaVersions.class);
            List<CatalogTableColumnDescriptor> columns = schemaVersions.latestVersionColumns();
            String storageProfile = input.readUTF();

            int schemaId = input.readVarIntAsInt();
            int pkIndexId = input.readVarIntAsInt();
            int zoneId = input.readVarIntAsInt();

            int pkKeysLen = input.readVarIntAsInt();
            int[] pkColumnIndexes = input.readIntArray(pkKeysLen);
            IntList primaryKeyColumns = new IntArrayList(pkColumnIndexes.length);

            for (int idx : pkColumnIndexes) {
                primaryKeyColumns.add(columns.get(idx).id());
            }

            int colocationColumnsLen = input.readVarIntAsInt();

            IntList colocationColumns;

            if (colocationColumnsLen == -1) {
                colocationColumns = primaryKeyColumns;
            } else {
                int[] colocationColumnIdxs = input.readIntArray(colocationColumnsLen);
                colocationColumns = resolveColumnIdsByIndexes(columns, colocationColumnIdxs);
            }

            double staleRowsFraction = input.readDouble();
            long minStaleRowsCount = input.readVarInt();

            return CatalogTableDescriptor.builder()
                    .id(id)
                    .schemaId(schemaId)
                    .primaryKeyIndexId(pkIndexId)
                    .name(name)
                    .zoneId(zoneId)
                    .primaryKeyColumns(primaryKeyColumns)
                    .colocationColumns(colocationColumns)
                    .schemaVersions(schemaVersions)
                    .storageProfile(storageProfile)
                    .timestamp(updateTimestamp)
                    .staleRowsFraction(staleRowsFraction)
                    .minStaleRowsCount(minStaleRowsCount)
                    .build();
        }

        @Override
        public void writeTo(CatalogTableDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());

            output.writeEntry(descriptor.schemaVersions());
            output.writeUTF(descriptor.storageProfile());

            output.writeVarInt(descriptor.schemaId());
            output.writeVarInt(descriptor.primaryKeyIndexId());
            output.writeVarInt(descriptor.zoneId());

            int[] pkIndexes = resolvePkColumnIndexes(descriptor);

            output.writeVarInt(pkIndexes.length);
            output.writeIntArray(pkIndexes);

            if (descriptor.colocationColumns() == descriptor.primaryKeyColumns()) {
                output.writeVarInt(-1);
            } else {
                int[] colocationIndexes = resolveColocationColumnIndexes(pkIndexes, descriptor);

                output.writeVarInt(colocationIndexes.length);
                output.writeIntArray(colocationIndexes);
            }

            output.writeDouble(descriptor.properties().staleRowsFraction());
            output.writeVarInt(descriptor.properties().minStaleRowsCount());
        }
    }

    /**
     * Return column ids for given column indexes.
     */
    private static IntList resolveColumnIdsByIndexes(List<CatalogTableColumnDescriptor> columns, int[] indexes) {
        IntList columnIds = new IntArrayList(indexes.length);

        for (int idx : indexes) {
            columnIds.add(columns.get(idx).id());
        }

        return columnIds;
    }

    /**
     * Return colocation key column positions in the column's list from given table descriptor.
     *
     * <p>Note: The methods accepts (precalculated) primary key column indexes as an optimization relying on the fact that
     * the colocation columns are a subset of primary key columns.
     */
    private static int[] resolveColocationColumnIndexes(int[] pkColumnIndexes, CatalogTableDescriptor descriptor) {
        List<CatalogTableColumnDescriptor> columns = descriptor.columns();
        IntList colocationIds = descriptor.colocationColumns();

        assert pkColumnIndexes.length >= colocationIds.size();

        int[] colocationColumnIndexes = new int[colocationIds.size()];
        int foundCount = 0;

        // Walk through PK columns as colocation columns are a subset of PK columns.
        for (int i = 0; i < pkColumnIndexes.length && foundCount < colocationColumnIndexes.length; i++) {
            int colIdx = pkColumnIndexes[i];
            int colId = columns.get(colIdx).id();

            int pos = colocationIds.indexOf(colId);
            if (pos != -1) {
                colocationColumnIndexes[pos] = colIdx;
                foundCount++;
            }
        }

        assert foundCount == colocationColumnIndexes.length;

        return colocationColumnIndexes;
    }

    /**
     * Return primary key column positions in the column list from given table descriptor.
     */
    private static int[] resolvePkColumnIndexes(CatalogTableDescriptor descriptor) {
        List<CatalogTableColumnDescriptor> columns = descriptor.columns();
        IntList pkColumnIds = descriptor.primaryKeyColumns();

        assert columns.size() >= pkColumnIds.size();

        int[] pkColumnIndexes = new int[pkColumnIds.size()];
        int foundCount = 0;

        for (int colIdx = 0; colIdx < columns.size() && foundCount < pkColumnIndexes.length; colIdx++) {
            int colId = columns.get(colIdx).id();

            int pos = pkColumnIds.indexOf(colId);
            if (pos != -1) {
                pkColumnIndexes[pos] = colIdx;
                foundCount++;
            }
        }

        assert foundCount == pkColumnIndexes.length;

        return pkColumnIndexes;
    }
}
