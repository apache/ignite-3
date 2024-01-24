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
import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.ConstantValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
import org.apache.ignite.internal.catalog.commands.DefaultValue.Type;
import org.apache.ignite.internal.catalog.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Table column descriptor.
 */
public class CatalogTableColumnDescriptor {
    public static CatalogObjectSerializer<CatalogTableColumnDescriptor> SERIALIZER = new TableColumnDescriptorSerializer();

    private final String name;
    private final ColumnType type;
    private final boolean nullable;
    /** Max length constraint. */
    private final int length;
    private final int precision;
    private final int scale;
    private final DefaultValue defaultValue;

    /**
     * Constructor.
     *
     * @param name Column name.
     * @param type Column type.
     * @param nullable Nullability flag.
     */
    public CatalogTableColumnDescriptor(String name, ColumnType type, boolean nullable,
            int precision, int scale, int length, @Nullable DefaultValue defaultValue) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type);
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
        this.length = length;
        this.defaultValue = defaultValue;
    }

    public String name() {
        return name;
    }

    public boolean nullable() {
        return nullable;
    }

    public ColumnType type() {
        return type;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    public int length() {
        return length;
    }

    @Nullable
    public DefaultValue defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CatalogTableColumnDescriptor that = (CatalogTableColumnDescriptor) o;

        if (nullable != that.nullable) {
            return false;
        }
        if (length != that.length) {
            return false;
        }
        if (precision != that.precision) {
            return false;
        }
        if (scale != that.scale) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        return defaultValue != null ? defaultValue.equals(that.defaultValue) : that.defaultValue == null;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (nullable ? 1 : 0);
        result = 31 * result + length;
        result = 31 * result + precision;
        result = 31 * result + scale;
        result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link CatalogTableColumnDescriptor}.
     */
    private static class TableColumnDescriptorSerializer implements CatalogObjectSerializer<CatalogTableColumnDescriptor> {
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
    private static class DefaultValueSerializer implements CatalogObjectSerializer<DefaultValue> {
        static DefaultValueSerializer INSTANCE = new DefaultValueSerializer();

        @Override
        @Nullable public DefaultValue readFrom(int version, IgniteDataInput in) throws IOException {
            int typeId = in.readInt();

            if (typeId == -1) {
                return null;
            }

            Type type = Type.forId(typeId);

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
                    throw new UnsupportedOperationException("Unexpected default value type; " + type);
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

                        break;
                    }

                    byte[] bytes = ByteUtils.toBytes(val);

                    out.writeInt(bytes.length);
                    out.writeByteArray(bytes);

                    break;

                default:
                    throw new UnsupportedOperationException("Unexpected default value type: " + value.type());
            }
        }
    }
}
