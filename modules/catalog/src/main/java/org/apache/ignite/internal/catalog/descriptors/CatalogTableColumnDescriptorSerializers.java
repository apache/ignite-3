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

import java.io.IOException;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.sql.ColumnType;

/**
 * Serializers for {@link CatalogTableColumnDescriptor}.
 */
public class CatalogTableColumnDescriptorSerializers {
    /**
     * Serializer for {@link CatalogTableColumnDescriptor}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class TableColumnDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogTableColumnDescriptor> {
        @Override
        public CatalogTableColumnDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            String name = input.readUTF();
            int typeId = input.readVarIntAsInt();
            ColumnType type = ColumnType.getById(typeId);

            assert type != null : "Unknown column type: " + typeId;

            boolean nullable = input.readBoolean();
            int precision = input.readVarIntAsInt();
            int scale = input.readVarIntAsInt();
            int length = input.readVarIntAsInt();

            DefaultValue defaultValue = DefaultValue.readFrom(input);

            return new CatalogTableColumnDescriptor(name, type, nullable, precision, scale, length, defaultValue);
        }

        @Override
        public void writeTo(CatalogTableColumnDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.type().id());
            output.writeBoolean(descriptor.nullable());
            output.writeVarInt(descriptor.precision());
            output.writeVarInt(descriptor.scale());
            output.writeVarInt(descriptor.length());

            DefaultValue.writeTo(descriptor.defaultValue(), output);
        }
    }

    @CatalogSerializer(version = 2, since = "3.1.0")
    static class TableColumnDescriptorSerializerV2 implements CatalogObjectSerializer<CatalogTableColumnDescriptor> {
        @Override
        public CatalogTableColumnDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            String name = input.readUTF();
            int typeId = input.readVarIntAsInt();
            ColumnType type = ColumnType.getById(typeId);

            assert type != null : "Unknown column type: " + typeId;

            boolean nullable = input.readBoolean();
            int precision = input.readVarIntAsInt();
            int scale = input.readVarIntAsInt();
            int length = input.readVarIntAsInt();

            DefaultValue defaultValue = DefaultValue.readFrom(input);

            return new CatalogTableColumnDescriptor(name, type, nullable, precision, scale, length, defaultValue);
        }

        @Override
        public void writeTo(CatalogTableColumnDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.type().id());
            output.writeBoolean(descriptor.nullable());
            output.writeVarInt(descriptor.precision());
            output.writeVarInt(descriptor.scale());
            output.writeVarInt(descriptor.length());

            DefaultValue.writeTo(descriptor.defaultValue(), output);
        }
    }

    @CatalogSerializer(version = 3, since = "3.2.0")
    static class TableColumnDescriptorSerializerV3 implements CatalogObjectSerializer<CatalogTableColumnDescriptor> {
        @Override
        public CatalogTableColumnDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            int typeId = input.readVarIntAsInt();
            ColumnType type = ColumnType.getById(typeId);

            assert type != null : "Unknown column type: " + typeId;

            boolean nullable = input.readBoolean();
            int precision = input.readVarIntAsInt();
            int scale = input.readVarIntAsInt();
            int length = input.readVarIntAsInt();

            DefaultValue defaultValue = DefaultValue.readFrom(input);

            return new CatalogTableColumnDescriptor(id, name, type, nullable, precision, scale, length, defaultValue);
        }

        @Override
        public void writeTo(CatalogTableColumnDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.type().id());
            output.writeBoolean(descriptor.nullable());
            output.writeVarInt(descriptor.precision());
            output.writeVarInt(descriptor.scale());
            output.writeVarInt(descriptor.length());

            DefaultValue.writeTo(descriptor.defaultValue(), output);
        }
    }
}
