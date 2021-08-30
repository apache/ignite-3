/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.proto.query.event;

import java.util.Objects;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC column metadata.
 */
public class JdbcColumnMeta extends JdbcResponse {
    /** Cache name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Column name. */
    private String colName;

    /** Data type. */
    private int dataType;

    /** Data type. */
    private String dataTypeName;

    /** Data type. */
    private String dataTypeClass;

    /**
     * Default constructor is used for serialization.
     */
    public JdbcColumnMeta() {
        // No-op.
    }

    /**
     * @param schemaName Schema.
     * @param tblName Table.
     * @param colName Column.
     * @param cls Type.
     */
    public JdbcColumnMeta(String schemaName, String tblName, String colName, Class<?> cls) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.colName = colName;

        String type = cls.getName();

        dataType = 0;//JdbcThinUtils.type(type);
        dataTypeName = null;//JdbcThinUtils.typeName(type);
        dataTypeClass = type;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return colName;
    }

    /**
     * @return Column's data type.
     */
    public int dataType() {
        return dataType;
    }

    /**
     * @return Column's data type name.
     */
    public String dataTypeName() {
        return dataTypeName;
    }

    /**
     * @return Column's data type class.
     */
    public String dataTypeClass() {
        return dataTypeClass;
    }

    /**
     * @return Column's default value.
     */
    public String defaultValue() {
        return null;
    }

    /**
     * @return Column's precision.
     */
    public int precision() {
        return -1;
    }

    /**
     * @return Column's scale.
     */
    public int scale() {
        return -1;
    }

    /**
     * Return 'nullable' flag in compatibility mode (according with column name and column type).
     *
     * @return {@code true} in case the column allows null values. Otherwise returns {@code false}
     */
    public boolean isNullable() {
        return nullable(colName, dataTypeClass);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (status() != STATUS_SUCCESS)
            return;

        packer.packString(schemaName);
        packer.packString(tblName);
        packer.packString(colName);

        packer.packInt(dataType);
        packer.packString(dataTypeName);
        packer.packString(dataTypeClass);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (status() != STATUS_SUCCESS)
            return;

        schemaName = unpacker.unpackString();
        tblName = unpacker.unpackString();
        colName = unpacker.unpackString();

        dataType = unpacker.unpackInt();
        dataTypeName = unpacker.unpackString();
        dataTypeClass = unpacker.unpackString();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcColumnMeta meta = (JdbcColumnMeta)o;

        return Objects.equals(schemaName, meta.schemaName)
            && Objects.equals(tblName, meta.tblName)
            && Objects.equals(colName, meta.colName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schemaName != null ? schemaName.hashCode() : 0;

        result = 31 * result + (tblName != null ? tblName.hashCode() : 0);
        result = 31 * result + colName.hashCode();

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcColumnMeta.class, this);
    }

    /**
     * Determines whether type is nullable.
     *
     * @param name Column name.
     * @param cls Java class name.
     * @return {@code True} if nullable.
     */
    private static boolean nullable(String name, String cls) {
        return !"_KEY".equalsIgnoreCase(name) &&
            !"_VAL".equalsIgnoreCase(name) &&
            !(boolean.class.getName().equals(cls) ||
                byte.class.getName().equals(cls) ||
                short.class.getName().equals(cls) ||
                int.class.getName().equals(cls) ||
                long.class.getName().equals(cls) ||
                float.class.getName().equals(cls) ||
                double.class.getName().equals(cls));
    }
}
