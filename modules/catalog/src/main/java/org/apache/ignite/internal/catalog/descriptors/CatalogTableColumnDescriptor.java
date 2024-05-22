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
import java.util.Objects;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Table column descriptor.
 */
public class CatalogTableColumnDescriptor {
    public static final CatalogObjectSerializer<CatalogTableColumnDescriptor> SERIALIZER = new TableColumnDescriptorSerializer();

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
        public CatalogTableColumnDescriptor readFrom(IgniteDataInput input) throws IOException {
            String name = input.readUTF();
            int typeId = input.readInt();
            ColumnType type = ColumnType.getById(typeId);

            assert type != null : "Unknown column type: " + typeId;

            boolean nullable = input.readBoolean();
            int precision = input.readInt();
            int scale = input.readInt();
            int length = input.readInt();

            DefaultValue defaultValue = DefaultValue.readFrom(input);

            return new CatalogTableColumnDescriptor(name, type, nullable, precision, scale, length, defaultValue);
        }

        @Override
        public void writeTo(CatalogTableColumnDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeUTF(descriptor.name());
            output.writeInt(descriptor.type().id());
            output.writeBoolean(descriptor.nullable());
            output.writeInt(descriptor.precision());
            output.writeInt(descriptor.scale());
            output.writeInt(descriptor.length());

            DefaultValue.writeTo(descriptor.defaultValue(), output);
        }
    }
}
