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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link ResultSetMetaData} implementations.
 */
public abstract class JdbcResultSetMetadataBaseSelfTest {

    protected static final ColumnDefinition COLUMN = new ColumnDefinition("L", ColumnType.DECIMAL, 10, 5, true);

    private static final int COLUMN_DISPLAY_SIZE = 30;

    protected abstract ResultSetMetaData createMeta(List<ColumnDefinition> columns);

    @Test
    public void columnCount() throws SQLException {
        {
            ResultSetMetaData md = createMeta(List.of(COLUMN));
            assertEquals(1, md.getColumnCount());
        }
        {
            ResultSetMetaData md = createMeta(List.of(COLUMN, COLUMN));
            assertEquals(2, md.getColumnCount());
        }
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void isAutoIncrement(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 0, 0, true);
        ResultSetMetaData md = createMeta(List.of(column));
        assertFalse(md.isAutoIncrement(1));
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void isCaseSensitive(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 0, 0, true);
        ResultSetMetaData md = createMeta(List.of(column));
        assertFalse(md.isCaseSensitive(1));
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void isSearchable(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 0, 0, true);
        ResultSetMetaData md = createMeta(List.of(column));
        assertFalse(md.isSearchable(1));
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void isCurrency(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 0, 0, true);
        ResultSetMetaData md = createMeta(List.of(column));
        assertFalse(md.isCurrency(1));
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void getColumnDisplaySize(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 0, 0, true);
        ResultSetMetaData md = createMeta(List.of(column));
        assertEquals(COLUMN_DISPLAY_SIZE, md.getColumnDisplaySize(1));
    }

    @Test
    public void getColumnLabel() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(
                new ColumnDefinition("LABEL1", ColumnType.INT8, 0, 0, true),
                new ColumnDefinition("label2", ColumnType.INT8, 0, 0, true).withOrigin("S", "T", null),
                new ColumnDefinition("Label3", ColumnType.INT8, 0, 0, true).withOrigin("S", "T", "COL")
        ));

        assertEquals("LABEL1", md.getColumnLabel(1));
        assertEquals("label2", md.getColumnLabel(2));
        assertEquals("Label3", md.getColumnLabel(3));
    }

    @Test
    public void getColumnName() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(
                new ColumnDefinition("COLUMN1", ColumnType.INT8, 0, 0, true),
                new ColumnDefinition("Column2", ColumnType.INT8, 0, 0, true).withOrigin("S", "T", null),
                new ColumnDefinition("C3", ColumnType.INT8, 0, 0, true).withOrigin("S", "T", "Column")
        ));

        assertEquals("COLUMN1", md.getColumnName(1));
        assertEquals("Column2", md.getColumnName(2));
        assertEquals("Column", md.getColumnName(3));
    }

    @Test
    public void getSchemaName() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(
                new ColumnDefinition("C1", ColumnType.INT8, 0, 0, true),
                new ColumnDefinition("C2", ColumnType.INT8, 0, 0, true).withOrigin("S", "T", null),
                new ColumnDefinition("Schema", ColumnType.INT8, 0, 0, true).withOrigin("Schema", "T", null)
        ));

        assertNull(md.getSchemaName(1));
        assertEquals("S", md.getSchemaName(2));
        assertEquals("Schema", md.getSchemaName(3));
    }

    @Test
    public void getTableName() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(
                new ColumnDefinition("C1", ColumnType.INT8, 0, 0, true),
                new ColumnDefinition("C2", ColumnType.INT8, 0, 0, true).withOrigin("S", "T", null),
                new ColumnDefinition("C3", ColumnType.INT8, 0, 0, true).withOrigin("S", "Table", null)
        ));

        assertNull(md.getTableName(1));
        assertEquals("T", md.getTableName(2));
        assertEquals("Table", md.getTableName(3));
    }

    @Test
    public void getCatalogName() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(COLUMN));
        assertEquals("", md.getCatalogName(1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void isNullable(boolean nullable) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", ColumnType.INT8, 0, 0, nullable);
        ResultSetMetaData md = createMeta(List.of(column));

        int indicator = nullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
        assertEquals(indicator, md.isNullable(1));
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void isSigned(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 0, 0, true);
        ResultSetMetaData md = createMeta(List.of(column));
        assertTrue(md.isSigned(1));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 10})
    public void getPrecision(int precision) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", ColumnType.DECIMAL, precision, 0, true);
        ResultSetMetaData md = createMeta(List.of(column));

        assertEquals(column.precision, md.getPrecision(1));
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void getColumnType(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 10, 5, true);

        ResultSetMetaData md = createMeta(List.of(column));
        assertEquals(JdbcColumnMeta.typeId(column.type), md.getColumnType(1));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 10})
    public void getScale(int scale) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", ColumnType.DECIMAL, 0, scale, true);
        ResultSetMetaData md = createMeta(List.of(column));

        assertEquals(column.scale, md.getScale(1));
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void getColumnTypeName(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 0, 0, true);

        ResultSetMetaData md = createMeta(List.of(column));
        assertEquals(JdbcColumnMeta.typeName(column.type), md.getColumnTypeName(1));
    }

    @Test
    public void isReadOnly() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(COLUMN));
        assertTrue(md.isReadOnly(1));
    }

    @Test
    public void isWritable() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(COLUMN));
        assertFalse(md.isWritable(1));
    }

    @Test
    public void isDefinitelyWritable() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(COLUMN));
        assertFalse(md.isDefinitelyWritable(1));
    }

    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void getColumnClassName(ColumnType columnType) throws SQLException {
        ColumnDefinition column = new ColumnDefinition("C", columnType, 0, 0, true);
        ResultSetMetaData md = createMeta(List.of(column));

        String typeClassName = JdbcConverterUtils.columnTypeToJdbcClass(column.type).getName();
        assertEquals(typeClassName, md.getColumnClassName(1));
    }

    @Test
    public void unwrapAndIsWrapperFor() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(COLUMN));
        assertTrue(md.isWrapperFor(ResultSetMetaData.class));
        assertDoesNotThrow(() -> md.unwrap(ResultSetMetaData.class));
    }

    @Test
    public void methodsExpectValidColumn() {
        ResultSetMetaData md = createMeta(List.of(COLUMN));

        expectInvalidColumnException(md::isAutoIncrement, 0);
        expectInvalidColumnException(md::isAutoIncrement, 2);

        expectInvalidColumnException(md::isCaseSensitive, 0);
        expectInvalidColumnException(md::isCaseSensitive, 2);

        expectInvalidColumnException(md::isSearchable, 0);
        expectInvalidColumnException(md::isSearchable, 2);

        expectInvalidColumnException(md::isCurrency, 0);
        expectInvalidColumnException(md::isCurrency, 2);

        expectInvalidColumnException(md::isNullable, 0);
        expectInvalidColumnException(md::isNullable, 2);

        expectInvalidColumnException(md::isSigned, 0);
        expectInvalidColumnException(md::isSigned, 2);

        expectInvalidColumnException(md::getColumnDisplaySize, 0);
        expectInvalidColumnException(md::getColumnDisplaySize, 2);

        expectInvalidColumnException(md::getColumnName, 0);
        expectInvalidColumnException(md::getColumnName, 2);

        expectInvalidColumnException(md::getSchemaName, 0);
        expectInvalidColumnException(md::getSchemaName, 2);

        expectInvalidColumnException(md::getPrecision, 0);
        expectInvalidColumnException(md::getPrecision, 2);

        expectInvalidColumnException(md::getScale, 0);
        expectInvalidColumnException(md::getScale, 2);

        expectInvalidColumnException(md::getTableName, 0);
        expectInvalidColumnException(md::getTableName, 2);

        expectInvalidColumnException(md::getCatalogName, 0);
        expectInvalidColumnException(md::getCatalogName, 2);

        expectInvalidColumnException(md::getColumnType, 0);
        expectInvalidColumnException(md::getColumnType, 2);

        expectInvalidColumnException(md::getColumnTypeName, 0);
        expectInvalidColumnException(md::getColumnTypeName, 2);

        expectInvalidColumnException(md::isReadOnly, 0);
        expectInvalidColumnException(md::isReadOnly, 2);

        expectInvalidColumnException(md::isWritable, 0);
        expectInvalidColumnException(md::isWritable, 2);

        expectInvalidColumnException(md::isDefinitelyWritable, 0);
        expectInvalidColumnException(md::isDefinitelyWritable, 2);

        expectInvalidColumnException(md::getColumnClassName, 0);
        expectInvalidColumnException(md::getColumnClassName, 2);
    }

    private static void expectInvalidColumnException(ColumnMetadataMethod m, int column) {
        SQLException err = assertThrows(SQLException.class, () -> m.call(column));
        assertThat(err.getMessage(), containsString("Invalid column index: " + column));
    }

    @FunctionalInterface
    private interface ColumnMetadataMethod {
        void call(int column) throws SQLException;
    }
}
