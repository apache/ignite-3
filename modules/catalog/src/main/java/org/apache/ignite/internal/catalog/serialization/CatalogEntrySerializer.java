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

package org.apache.ignite.internal.catalog.serialization;

import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readArray;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readNullableString;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readStringList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeArray;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeNullableString;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeStringCollection;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.ConstantValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
import org.apache.ignite.internal.catalog.commands.DefaultValue.Type;
import org.apache.ignite.internal.catalog.descriptors.CatalogDataStorageDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.serialization.DescriptorHeaderSerializer.CatalogDescriptorHeader;
import org.apache.ignite.internal.catalog.serialization.NewIndexEntrySerializer.IndexDescriptorSerializer;
import org.apache.ignite.internal.catalog.storage.AlterColumnEntry;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.DropColumnsEntry;
import org.apache.ignite.internal.catalog.storage.DropIndexEntry;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
import org.apache.ignite.internal.catalog.storage.DropZoneEntry;
import org.apache.ignite.internal.catalog.storage.MakeIndexAvailableEntry;
import org.apache.ignite.internal.catalog.storage.NewColumnsEntry;
import org.apache.ignite.internal.catalog.storage.NewSystemViewEntry;
import org.apache.ignite.internal.catalog.storage.NewTableEntry;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.RenameTableEntry;
import org.apache.ignite.internal.catalog.storage.StartBuildingIndexEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog entry serializer.
 */
public interface CatalogEntrySerializer<T> {
    T readFrom(int version, IgniteDataInput input) throws IOException;

    void writeTo(T value, int version, IgniteDataOutput output) throws IOException;

    /** Returns entry serializer for the specified type. */
    static <T extends UpdateEntry> CatalogEntrySerializer<T> forTypeId(short typeId) {
        UpdateEntryType type = UpdateEntryType.getById(typeId);

        if (type == null) {
            throw new IllegalArgumentException("Unknown entry type " + typeId);
        }

        switch (type) {
            case ALTER_ZONE:
                return (CatalogEntrySerializer<T>) AlterZoneEntrySerializer.INSTANCE;

            case NEW_ZONE:
                return (CatalogEntrySerializer<T>) NewZoneEntrySerializer.INSTANCE;

            case ALTER_COLUMN:
                return (CatalogEntrySerializer<T>) AlterColumnEntrySerializer.INSTANCE;

            case DROP_COLUMN:
                return (CatalogEntrySerializer<T>) DropColumnEntrySerializer.INSTANCE;

            case DROP_INDEX:
                return (CatalogEntrySerializer<T>) DropIndexEntrySerializer.INSTANCE;

            case DROP_TABLE:
                return (CatalogEntrySerializer<T>) DropTableEntrySerializer.INSTANCE;

            case DROP_ZONE:
                return (CatalogEntrySerializer<T>) DropZoneEntrySerializer.INSTANCE;

            case MAKE_INDEX_AVAILABLE:
                return (CatalogEntrySerializer<T>) MakeIndexAvailableEntrySerializer.INSTANCE;

            case START_BUILDING_INDEX:
                return (CatalogEntrySerializer<T>) StartBuildingIndexEntrySerializer.INSTANCE;

            case NEW_COLUMN:
                return (CatalogEntrySerializer<T>) NewColumnsEntrySerializer.INSTANCE;

            case NEW_INDEX:
                return (CatalogEntrySerializer<T>) NewIndexEntrySerializer.INSTANCE;

            case NEW_SYS_VIEW:
                return (CatalogEntrySerializer<T>) NewSystemViewEntrySerializer.INSTANCE;

            case NEW_TABLE:
                return (CatalogEntrySerializer<T>) NewTableEntrySerializer.INSTANCE;

            case RENAME_TABLE:
                return (CatalogEntrySerializer<T>) RenameTableEntrySerializer.INSTANCE;

            case ID_GENERATOR:
                return (CatalogEntrySerializer<T>) ObjectIdGenUpdateEntrySerializer.INSTANCE;

            default:
                throw new UnsupportedOperationException("Serialization is not supported for type: " + type);
        }
    }

    /**
     * Serializer for {@link ObjectIdGenUpdateEntry}.
     */
    class ObjectIdGenUpdateEntrySerializer implements CatalogEntrySerializer<ObjectIdGenUpdateEntry> {
        static ObjectIdGenUpdateEntrySerializer INSTANCE = new ObjectIdGenUpdateEntrySerializer();

        @Override
        public ObjectIdGenUpdateEntry readFrom(int version, IgniteDataInput input) throws IOException {
            int delta = input.readInt();

            return new ObjectIdGenUpdateEntry(delta);
        }

        @Override
        public void writeTo(ObjectIdGenUpdateEntry entry, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(entry.delta());
        }
    }

    /**
     * Serializer for {@link RenameTableEntry}.
     */
    class RenameTableEntrySerializer implements CatalogEntrySerializer<RenameTableEntry> {
        static RenameTableEntrySerializer INSTANCE = new RenameTableEntrySerializer();

        @Override
        public RenameTableEntry readFrom(int version, IgniteDataInput input) throws IOException {
            int tableId = input.readInt();
            String newTableName = input.readUTF();

            return new RenameTableEntry(tableId, newTableName);
        }

        @Override
        public void writeTo(RenameTableEntry entry, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(entry.tableId());
            output.writeUTF(entry.newTableName());
        }
    }

    /**
     * Serializer for {@link NewTableEntry}.
     */
    class NewTableEntrySerializer implements CatalogEntrySerializer<NewTableEntry> {
        static NewTableEntrySerializer INSTANCE = new NewTableEntrySerializer();

        @Override
        public NewTableEntry readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogTableDescriptor descriptor = TableDescriptorSerializer.INSTANCE.readFrom(version, input);
            String schemaName = input.readUTF();

            return new NewTableEntry(descriptor, schemaName);
        }

        @Override
        public void writeTo(NewTableEntry entry, int version, IgniteDataOutput output) throws IOException {
            TableDescriptorSerializer.INSTANCE.writeTo(entry.descriptor(), version, output);
            output.writeUTF(entry.schemaName());
        }
    }

    /**
     * Serializer for {@link CatalogTableDescriptor}.
     */
    class TableDescriptorSerializer implements CatalogEntrySerializer<CatalogTableDescriptor> {
        static TableDescriptorSerializer INSTANCE = new TableDescriptorSerializer();

        @Override
        public CatalogTableDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogDescriptorHeader header = DescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
            CatalogTableSchemaVersions schemaVersions = TableSchemaVersionsSerializer.INSTANCE.readFrom(version, input);
            List<CatalogTableColumnDescriptor> columns = readList(version, input, TableColumnDescriptorSerializer.INSTANCE);

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
            DescriptorHeaderSerializer.INSTANCE.writeTo(new CatalogDescriptorHeader(descriptor), version, output);
            TableSchemaVersionsSerializer.INSTANCE.writeTo(descriptor.schemaVersions(), version, output);
            writeList(descriptor.columns(), version, TableColumnDescriptorSerializer.INSTANCE, output);

            output.writeInt(descriptor.schemaId());
            output.writeInt(descriptor.primaryKeyIndexId());
            output.writeInt(descriptor.zoneId());
            writeStringCollection(descriptor.primaryKeyColumns(), output);
            writeStringCollection(descriptor.colocationColumns(), output);
            output.writeLong(descriptor.creationToken());
        }
    }

    /**
     * Serializer for {@link CatalogTableSchemaVersions}.
     */
    class TableSchemaVersionsSerializer implements CatalogEntrySerializer<CatalogTableSchemaVersions> {
        static TableSchemaVersionsSerializer INSTANCE = new TableSchemaVersionsSerializer();

        @Override
        public CatalogTableSchemaVersions readFrom(int version, IgniteDataInput input) throws IOException {
            TableVersion[] versions = readArray(version, input, TableVersionSerializer.INSTANCE, TableVersion.class);
            int base = input.readInt();

            return new CatalogTableSchemaVersions(base, versions);
        }

        @Override
        public void writeTo(CatalogTableSchemaVersions tabVersions, int version, IgniteDataOutput output) throws IOException {
            writeArray(tabVersions.versions(), version, TableVersionSerializer.INSTANCE, output);
            output.writeInt(tabVersions.base());
        }
    }

    /**
     * Serializer for {@link TableVersion}.
     */
    class TableVersionSerializer implements CatalogEntrySerializer<TableVersion> {
        static TableVersionSerializer INSTANCE = new TableVersionSerializer();

        @Override
        public TableVersion readFrom(int version, IgniteDataInput input) throws IOException {
            List<CatalogTableColumnDescriptor> columns = readList(version, input, TableColumnDescriptorSerializer.INSTANCE);

            return new TableVersion(columns);
        }

        @Override
        public void writeTo(TableVersion tableVersion, int version, IgniteDataOutput output) throws IOException {
            writeList(tableVersion.columns(), version, TableColumnDescriptorSerializer.INSTANCE, output);
        }
    }

    /**
     * Serializer for {@link CatalogSchemaDescriptor}.
     */
    class SchemaDescriptorSerializer implements CatalogEntrySerializer<CatalogSchemaDescriptor> {
        static SchemaDescriptorSerializer INSTANCE = new SchemaDescriptorSerializer();

        @Override
        public CatalogSchemaDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogDescriptorHeader header = DescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
            CatalogTableDescriptor[] tables = readArray(version, input, TableDescriptorSerializer.INSTANCE, CatalogTableDescriptor.class);
            CatalogIndexDescriptor[] indexes = readArray(version, input, IndexDescriptorSerializer.INSTANCE, CatalogIndexDescriptor.class);
            CatalogSystemViewDescriptor[] systemViews =
                    readArray(version, input, SystemViewDescriptorSerializer.INSTANCE, CatalogSystemViewDescriptor.class);

            return new CatalogSchemaDescriptor(header.id(), header.name(), tables, indexes, systemViews, header.updateToken());
        }

        @Override
        public void writeTo(CatalogSchemaDescriptor value, int version, IgniteDataOutput output) throws IOException {
            DescriptorHeaderSerializer.INSTANCE.writeTo(new CatalogDescriptorHeader(value), version, output);
            writeArray(value.tables(), version, TableDescriptorSerializer.INSTANCE, output);
            writeArray(value.indexes(), version, IndexDescriptorSerializer.INSTANCE, output);
            writeArray(value.systemViews(), version, SystemViewDescriptorSerializer.INSTANCE, output);
        }
    }

    /**
     * Serializer for {@link NewSystemViewEntry}.
     */
    class NewSystemViewEntrySerializer implements CatalogEntrySerializer<NewSystemViewEntry> {
        static NewSystemViewEntrySerializer INSTANCE = new NewSystemViewEntrySerializer();

        @Override
        public NewSystemViewEntry readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogSystemViewDescriptor descriptor = SystemViewDescriptorSerializer.INSTANCE.readFrom(version, input);
            String schema = input.readUTF();

            return new NewSystemViewEntry(descriptor, schema);
        }

        @Override
        public void writeTo(NewSystemViewEntry entry, int version, IgniteDataOutput output) throws IOException {
            SystemViewDescriptorSerializer.INSTANCE.writeTo(entry.descriptor(), version, output);
            output.writeUTF(entry.schemaName());
        }
    }

    /**
     * Serializer for {@link CatalogSystemViewDescriptor}.
     */
    class SystemViewDescriptorSerializer implements CatalogEntrySerializer<CatalogSystemViewDescriptor> {
        static SystemViewDescriptorSerializer INSTANCE = new SystemViewDescriptorSerializer();

        @Override
        public CatalogSystemViewDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogDescriptorHeader header = DescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
            List<CatalogTableColumnDescriptor> columns = readList(version, input, TableColumnDescriptorSerializer.INSTANCE);

            byte sysViewTypeId = input.readByte();
            SystemViewType sysViewType = SystemViewType.getById(sysViewTypeId);

            return new CatalogSystemViewDescriptor(header.id(), header.name(), columns, sysViewType, header.updateToken());
        }

        @Override
        public void writeTo(CatalogSystemViewDescriptor descriptor, int version, IgniteDataOutput output) throws IOException {
            DescriptorHeaderSerializer.INSTANCE.writeTo(new CatalogDescriptorHeader(descriptor), version, output);
            writeList(descriptor.columns(), version, TableColumnDescriptorSerializer.INSTANCE, output);
            output.writeByte(descriptor.systemViewType().id());
        }
    }

    /**
     * Serializer for {@link NewColumnsEntry}.
     */
    class NewColumnsEntrySerializer implements CatalogEntrySerializer<NewColumnsEntry> {
        static NewColumnsEntrySerializer INSTANCE = new NewColumnsEntrySerializer();

        @Override
        public NewColumnsEntry readFrom(int version, IgniteDataInput in) throws IOException {
            List<CatalogTableColumnDescriptor> columns = readList(version, in, TableColumnDescriptorSerializer.INSTANCE);
            int tableId = in.readInt();
            String schemaName = in.readUTF();

            return new NewColumnsEntry(tableId, columns, schemaName);
        }

        @Override
        public void writeTo(NewColumnsEntry entry, int version, IgniteDataOutput out) throws IOException {
            writeList(entry.descriptors(), version, TableColumnDescriptorSerializer.INSTANCE, out);
            out.writeInt(entry.tableId());
            out.writeUTF(entry.schemaName());
        }
    }

    /**
     * Serializer for {@link DropIndexEntry}.
     */
    class DropIndexEntrySerializer implements CatalogEntrySerializer<DropIndexEntry> {
        static DropIndexEntrySerializer INSTANCE = new DropIndexEntrySerializer();

        @Override
        public DropIndexEntry readFrom(int version, IgniteDataInput input) throws IOException {
            int indexId = input.readInt();
            int tableId = input.readInt();
            String schemaName = input.readUTF();

            return new DropIndexEntry(indexId, tableId, schemaName);
        }

        @Override
        public void writeTo(DropIndexEntry entry, int version, IgniteDataOutput out) throws IOException {
            out.writeInt(entry.indexId());
            out.writeInt(entry.tableId());
            out.writeUTF(entry.schemaName());
        }
    }

    /**
     * Serializer for {@link DropTableEntry}.
     */
    class DropTableEntrySerializer implements CatalogEntrySerializer<DropTableEntry> {
        static DropTableEntrySerializer INSTANCE = new DropTableEntrySerializer();

        @Override
        public DropTableEntry readFrom(int version, IgniteDataInput input) throws IOException {
            int tableId = input.readInt();
            String schemaName = input.readUTF();

            return new DropTableEntry(tableId, schemaName);
        }

        @Override
        public void writeTo(DropTableEntry entry, int version, IgniteDataOutput out) throws IOException {
            out.writeInt(entry.tableId());
            out.writeUTF(entry.schemaName());
        }
    }

    /**
     * Serializer for {@link DropZoneEntry}.
     */
    class DropZoneEntrySerializer implements CatalogEntrySerializer<DropZoneEntry> {
        static DropZoneEntrySerializer INSTANCE = new DropZoneEntrySerializer();

        @Override
        public DropZoneEntry readFrom(int version, IgniteDataInput input) throws IOException {
            int zoneId = input.readInt();

            return new DropZoneEntry(zoneId);
        }

        @Override
        public void writeTo(DropZoneEntry entry, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(entry.zoneId());
        }
    }

    /**
     * Serializer for {@link MakeIndexAvailableEntry}.
     */
    class MakeIndexAvailableEntrySerializer implements CatalogEntrySerializer<MakeIndexAvailableEntry> {
        static MakeIndexAvailableEntrySerializer INSTANCE = new MakeIndexAvailableEntrySerializer();

        @Override
        public MakeIndexAvailableEntry readFrom(int version, IgniteDataInput input) throws IOException {
            int indexId = input.readInt();

            return new MakeIndexAvailableEntry(indexId);
        }

        @Override
        public void writeTo(MakeIndexAvailableEntry object, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(object.indexId());
        }
    }

    /**
     * Serializer for {@link StartBuildingIndexEntry}.
     */
    class StartBuildingIndexEntrySerializer implements CatalogEntrySerializer<StartBuildingIndexEntry> {
        static StartBuildingIndexEntrySerializer INSTANCE = new StartBuildingIndexEntrySerializer();

        @Override
        public StartBuildingIndexEntry readFrom(int version, IgniteDataInput input) throws IOException {
            int indexId = input.readInt();

            return new StartBuildingIndexEntry(indexId);
        }

        @Override
        public void writeTo(StartBuildingIndexEntry object, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(object.indexId());
        }
    }

    /**
     * Serializer for {@link DropColumnsEntry}.
     */
    class DropColumnEntrySerializer implements CatalogEntrySerializer<DropColumnsEntry> {
        static DropColumnEntrySerializer INSTANCE = new DropColumnEntrySerializer();

        @Override
        public DropColumnsEntry readFrom(int version, IgniteDataInput in) throws IOException {
            String schemaName = in.readUTF();
            int tableId = in.readInt();
            Set<String> columns = CatalogSerializationUtils.readStringSet(in);

            return new DropColumnsEntry(tableId, columns, schemaName);
        }

        @Override
        public void writeTo(DropColumnsEntry object, int version, IgniteDataOutput out) throws IOException {
            out.writeUTF(object.schemaName());
            out.writeInt(object.tableId());
            writeStringCollection(object.columns(), out);
        }
    }

    /**
     * Serializer for {@link AlterColumnEntry}.
     */
    class AlterColumnEntrySerializer implements CatalogEntrySerializer<AlterColumnEntry> {
        static AlterColumnEntrySerializer INSTANCE = new AlterColumnEntrySerializer();

        @Override
        public AlterColumnEntry readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogTableColumnDescriptor descriptor = TableColumnDescriptorSerializer.INSTANCE.readFrom(version, input);

            String schemaName = input.readUTF();
            int tableId = input.readInt();

            return new AlterColumnEntry(tableId, descriptor, schemaName);
        }

        @Override
        public void writeTo(AlterColumnEntry value, int version, IgniteDataOutput output) throws IOException {
            TableColumnDescriptorSerializer.INSTANCE.writeTo(value.descriptor(), version, output);

            output.writeUTF(value.schemaName());
            output.writeInt(value.tableId());
        }
    }

    /**
     * Serializer for {@link CatalogTableColumnDescriptor}.
     */
    class TableColumnDescriptorSerializer implements CatalogEntrySerializer<CatalogTableColumnDescriptor> {
        static TableColumnDescriptorSerializer INSTANCE = new TableColumnDescriptorSerializer();

        @Override
        public CatalogTableColumnDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            DefaultValue defaultValue = DefaultValueSerializer.INSTANCE.readFrom(version, input);
            String name = input.readUTF();
            int typeId = input.readInt();

            ColumnType type = ColumnType.getById(typeId);

            assert type != null : typeId;

            boolean nullable = input.readBoolean();
            int precision = input.readInt();
            int scale = input.readInt();
            int length = input.readInt();

            return new CatalogTableColumnDescriptor(name, type, nullable, precision, scale, length, defaultValue);
        }

        @Override
        public void writeTo(CatalogTableColumnDescriptor descriptor, int version, IgniteDataOutput output) throws IOException {
            DefaultValueSerializer.INSTANCE.writeTo(descriptor.defaultValue(), version, output);

            output.writeUTF(descriptor.name());
            output.writeInt(descriptor.type().id());
            output.writeBoolean(descriptor.nullable());
            output.writeInt(descriptor.precision());
            output.writeInt(descriptor.scale());
            output.writeInt(descriptor.length());
        }
    }

    /**
     * Serializer for {@link DefaultValue}.
     */
    class DefaultValueSerializer implements CatalogEntrySerializer<DefaultValue> {
        static DefaultValueSerializer INSTANCE = new DefaultValueSerializer();

        @Override
        @Nullable public DefaultValue readFrom(int version, IgniteDataInput in) throws IOException {
            int typeId = in.readInt();

            if (typeId == -1) {
                return null;
            }

            Type type = Type.getById(typeId);

            switch (type) {
                case FUNCTION_CALL:
                    return FunctionCall.functionCall(in.readUTF());
                case CONSTANT:
                    int length = in.readInt();

                    if (length == -1) {
                        return DefaultValue.constant(null);
                    }

                    byte[] bytes = in.readByteArray(length);

                    return DefaultValue.constant(ByteUtils.fromBytes(bytes));

                default:
                    throw new UnsupportedOperationException("Unsupported default value type; " + type);
            }
        }

        @Override
        public void writeTo(@Nullable DefaultValue value, int version, IgniteDataOutput out) throws IOException {
            if (value == null) {
                out.writeInt(-1);

                return;
            }

            out.writeInt(value.type().id());

            switch (value.type()) {
                case FUNCTION_CALL:
                    out.writeUTF(((FunctionCall) value).functionName());

                    break;
                case CONSTANT:
                    ConstantValue constValue = (ConstantValue) value;

                    Serializable val = constValue.value();

                    if (val == null) {
                        out.writeInt(-1);
                    } else {
                        byte[] bytes = ByteUtils.toBytes(val);

                        out.writeInt(bytes.length);

                        out.writeByteArray(bytes);
                    }
                    break;

                default:
                    throw new UnsupportedOperationException("Unsupported default value type: " + value.type());
            }
        }
    }

    /**
     * Serializer for {@link AlterZoneEntry}.
     */
    class AlterZoneEntrySerializer implements CatalogEntrySerializer<AlterZoneEntry> {
        static AlterZoneEntrySerializer INSTANCE = new AlterZoneEntrySerializer();

        @Override
        public AlterZoneEntry readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogZoneDescriptor descriptor = ZoneDescriptorSerializer.INSTANCE.readFrom(version, input);

            return new AlterZoneEntry(descriptor);
        }

        @Override
        public void writeTo(AlterZoneEntry object, int version, IgniteDataOutput output) throws IOException {
            ZoneDescriptorSerializer.INSTANCE.writeTo(object.descriptor(), version, output);
        }
    }

    /**
     * Serializer for {@link NewZoneEntry}.
     */
    class NewZoneEntrySerializer implements CatalogEntrySerializer<NewZoneEntry> {
        static NewZoneEntrySerializer INSTANCE = new NewZoneEntrySerializer();

        @Override
        public NewZoneEntry readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogZoneDescriptor descriptor = ZoneDescriptorSerializer.INSTANCE.readFrom(version, input);

            return new NewZoneEntry(descriptor);
        }

        @Override
        public void writeTo(NewZoneEntry object, int version, IgniteDataOutput output) throws IOException {
            ZoneDescriptorSerializer.INSTANCE.writeTo(object.descriptor(), version, output);
        }
    }

    /**
     * Serializer for {@link CatalogDataStorageDescriptor}.
     */
    class DataStorageDescriptorSerializer implements CatalogEntrySerializer<CatalogDataStorageDescriptor> {
        static DataStorageDescriptorSerializer INSTANCE = new DataStorageDescriptorSerializer();

        @Override
        public CatalogDataStorageDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            if (!input.readBoolean()) {
                return null;
            }

            String engine = input.readUTF();
            String region = readNullableString(input);

            return new CatalogDataStorageDescriptor(engine, region);
        }

        @Override
        public void writeTo(CatalogDataStorageDescriptor descriptor, int version, IgniteDataOutput output) throws IOException {
            output.writeBoolean(descriptor != null);

            if (descriptor != null) {
                output.writeUTF(descriptor.engine());
                writeNullableString(descriptor.dataRegion(), output);
            }
        }
    }

    /**
     * Serializer for {@link CatalogZoneDescriptor}.
     */
    class ZoneDescriptorSerializer implements CatalogEntrySerializer<CatalogZoneDescriptor> {
        static ZoneDescriptorSerializer INSTANCE = new ZoneDescriptorSerializer();

        @Override
        public CatalogZoneDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            DescriptorHeaderSerializer.CatalogDescriptorHeader header = DescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
            CatalogDataStorageDescriptor dataStorageDescriptor = DataStorageDescriptorSerializer.INSTANCE.readFrom(version, input);

            int partitions = input.readInt();
            int replicas = input.readInt();
            int dataNodesAutoAdjust = input.readInt();
            int dataNodesAutoAdjustScaleUp = input.readInt();
            int dataNodesAutoAdjustScaleDown = input.readInt();
            String filter = input.readUTF();

            return new CatalogZoneDescriptor(
                    header.id(),
                    header.name(),
                    partitions,
                    replicas,
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    dataStorageDescriptor,
                    header.updateToken()
            );
        }

        @Override
        public void writeTo(CatalogZoneDescriptor value, int version, IgniteDataOutput out) throws IOException {
            DescriptorHeaderSerializer.INSTANCE.writeTo(new DescriptorHeaderSerializer.CatalogDescriptorHeader(value), version, out);
            DataStorageDescriptorSerializer.INSTANCE.writeTo(value.dataStorage(), version, out);

            out.writeInt(value.partitions());
            out.writeInt(value.replicas());
            out.writeInt(value.dataNodesAutoAdjust());
            out.writeInt(value.dataNodesAutoAdjustScaleUp());
            out.writeInt(value.dataNodesAutoAdjustScaleDown());
            out.writeUTF(value.filter());
        }
    }
}
