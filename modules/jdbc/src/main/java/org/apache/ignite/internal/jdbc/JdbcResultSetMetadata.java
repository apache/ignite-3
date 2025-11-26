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

package org.apache.ignite.internal.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnMetadata.ColumnOrigin;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * JDBC result set metadata implementation.
 */
public class JdbcResultSetMetadata implements ResultSetMetaData {
    private static final int COLUMN_DISPLAY_SIZE = 30;

    private final List<ColumnMetadata> cols;

    /**
     * Constructor.
     *
     * @param metadata Metadata.
     */
    public JdbcResultSetMetadata(ResultSetMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata");
        }
        this.cols = metadata.columns();
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnCount() throws SQLException {
        return cols.size();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSearchable(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCurrency(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public int isNullable(int column) throws SQLException {
        return getColumn(column).nullable() ? columnNullable : columnNoNulls;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSigned(int column) throws SQLException {
        getColumn(column);
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        getColumn(column);
        return COLUMN_DISPLAY_SIZE;
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnLabel(int column) throws SQLException {
        ColumnMetadata metadata = getColumn(column);
        return metadata.name();
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnName(int column) throws SQLException {
        ColumnMetadata metadata = getColumn(column);
        ColumnOrigin origin = metadata.origin();
        // Compatibility with the existing driver
        if (origin != null && origin.columnName() != null) {
            return origin.columnName();
        } else {
            return metadata.name();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String getSchemaName(int column) throws SQLException {
        ColumnMetadata.ColumnOrigin origin = getColumn(column).origin();
        // Compatibility with the existing driver
        return origin != null ? origin.schemaName() : null;
    }

    /** {@inheritDoc} */
    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumn(column).precision();
    }

    /** {@inheritDoc} */
    @Override
    public int getScale(int column) throws SQLException {
        return getColumn(column).scale();
    }

    /** {@inheritDoc} */
    @Override
    public String getTableName(int column) throws SQLException {
        ColumnMetadata.ColumnOrigin origin = getColumn(column).origin();
        // Compatibility with the existing driver
        return origin != null ? origin.tableName() : null;
    }

    /** {@inheritDoc} */
    @Override
    public String getCatalogName(int column) throws SQLException {
        getColumn(column);
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnType(int column) throws SQLException {
        ColumnType columnType = getColumn(column).type();
        return JdbcColumnMeta.typeId(columnType);
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        ColumnType columnType = getColumn(column).type();
        return JdbcColumnMeta.typeName(columnType);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        getColumn(column);
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWritable(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnClassName(int column) throws SQLException {
        ColumnType columnType = getColumn(column).type();
        return JdbcConverterUtils.columnTypeToJdbcClass(columnType).getName();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface)) {
            throw new SQLException("Result set meta data is not a wrapper for " + iface.getName());
        }
        return iface.cast(this);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcResultSetMetadata.class);
    }

    private ColumnMetadata getColumn(int idx) throws SQLException {
        // JDBC columns are 1-based
        if (idx < 1 || idx > cols.size()) {
            throw new SQLException("Invalid column index: " + idx);
        }
        return cols.get(idx - 1);
    }
}
