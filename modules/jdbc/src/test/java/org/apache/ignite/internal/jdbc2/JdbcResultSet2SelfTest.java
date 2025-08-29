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

package org.apache.ignite.internal.jdbc2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.jdbc.ColumnDefinition;
import org.apache.ignite.internal.jdbc.JdbcResultSetBaseSelfTest;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Runs JdbcResultSetCompatibilityBaseTest against modern org.apache.ignite.internal.jdbc2.JdbcResultSet.
 */
public class JdbcResultSet2SelfTest extends JdbcResultSetBaseSelfTest {

    private static final int FETCH_SIZE = 10;

    @Test
    public void unwrap() throws SQLException {
        try (ResultSet rs = createPositionedSingle(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), true)) {
            {
                assertTrue(rs.isWrapperFor(JdbcResultSet.class));
                JdbcResultSet unwrapped = rs.unwrap(JdbcResultSet.class);
                assertNotNull(unwrapped);
            }

            {
                assertTrue(rs.isWrapperFor(ResultSet.class));
                ResultSet unwrapped = rs.unwrap(ResultSet.class);
                assertNotNull(unwrapped);
            }

            {
                assertFalse(rs.isWrapperFor(Connection.class));
                SQLException err = assertThrows(SQLException.class, () -> rs.unwrap(Connection.class));
                assertThat(err.getMessage(), containsString("Result set is not a wrapper for " + Connection.class.getName()));
            }
        }
    }

    @Test
    @Override
    public void getMetadata() throws SQLException {
        try (ResultSet rs = createPositionedSingle(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), true)) {
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals(1, metaData.getColumnCount());
        }

        try (ResultSet rs = createResultSet(null, List.of(), List.of())) {
            expectSqlException(rs::getMetaData, "ResultSet doesn't have metadata");
        }
    }

    @Test
    public void closedWhenStatementIsClosed() throws SQLException {
        Statement statement = Mockito.mock(Statement.class);
        when(statement.isClosed()).thenReturn(true);

        try (ResultSet rs = createResultSet(statement, null,
                List.of(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false)),
                List.of(List.of()))
        ) {
            assertTrue(rs.isClosed());
        }
    }

    @Override
    protected ResultSet createResultSet(@Nullable ZoneId zoneId, List<ColumnDefinition> cols, List<List<Object>> rows) {
        Statement statement = Mockito.mock(Statement.class);

        return createResultSet(statement, zoneId, cols, rows);
    }

    @SuppressWarnings("unchecked")
    private static ResultSet createResultSet(
            Statement statement,
            @Nullable ZoneId zoneId,
            List<ColumnDefinition> cols,
            List<List<Object>> rows
    ) {

        // ResultSet has no metadata
        if (cols.isEmpty() && rows.isEmpty()) {
            org.apache.ignite.sql.ResultSet<SqlRow> rs = Mockito.mock(org.apache.ignite.sql.ResultSet.class);
            when(rs.metadata()).thenReturn(null);

            return new JdbcResultSet(rs, statement, () -> zoneId, FETCH_SIZE);
        }

        List<ColumnMetadata> apiCols = new ArrayList<>();
        for (ColumnDefinition c : cols) {
            String schema = c.schema;
            String table = c.table;
            String column = c.column != null ? c.column : c.label.toUpperCase(Locale.US);
            boolean nullable = true;
            ColumnOriginImpl origin = new ColumnOriginImpl(schema, table, column);
            apiCols.add(new ColumnMetadataImpl(c.label, c.type, c.precision, c.scale, nullable, origin));
        }

        ResultSetMetadata apiMeta = new ResultSetMetadataImpl(apiCols);

        return new JdbcResultSet(new ApiResultSetStub(apiMeta, rows), statement, () -> zoneId, FETCH_SIZE);
    }

    /** Minimal synchronous implementation of org.apache.ignite.sql.ResultSet{@literal <SqlRow>} for tests. */
    protected static class ApiResultSetStub implements org.apache.ignite.sql.ResultSet<SqlRow> {
        private final ResultSetMetadata meta;
        private final Iterator<List<Object>> it;
        private List<Object> current;

        ApiResultSetStub(ResultSetMetadata meta, List<List<Object>> rows) {
            this.meta = Objects.requireNonNull(meta, "meta");
            this.it = rows.iterator();
            this.current = null;
        }

        @Override
        public ResultSetMetadata metadata() {
            return meta;
        }

        @Override
        public boolean hasRowSet() {
            return true;
        }

        @Override
        public long affectedRows() {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public boolean wasApplied() {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public SqlRow next() {
            if (!it.hasNext()) {
                throw new NoSuchElementException();
            }
            current = it.next();
            return new UnmodifiableSqlRow(current, meta);
        }
    }

    /** Minimal SqlRow implementation backed by simple arrays for tests. */
    private static class UnmodifiableSqlRow implements SqlRow {
        private final List<Object> values;
        private final ResultSetMetadata meta;

        UnmodifiableSqlRow(List<Object> values, ResultSetMetadata meta) {
            this.values = values;
            this.meta = meta;
        }

        @Override
        public ResultSetMetadata metadata() {
            return meta;
        }

        @Override
        public int columnCount() {
            return meta.columns().size();
        }

        @Override
        public String columnName(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public int columnIndex(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public <T> T valueOrDefault(String columnName, T defaultValue) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public Tuple set(String columnName, Object value) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public <T> T value(String columnName) throws IllegalArgumentException {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public <T> T value(int columnIndex) {
            return (T) values.get(columnIndex);
        }

        @Override
        public boolean booleanValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public boolean booleanValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public byte byteValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public byte byteValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public short shortValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public short shortValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public int intValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public int intValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public long longValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public long longValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public float floatValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public float floatValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public double doubleValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public double doubleValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public BigDecimal decimalValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public BigDecimal decimalValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public String stringValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public String stringValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public byte[] bytesValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public byte[] bytesValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public UUID uuidValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public UUID uuidValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public LocalDate dateValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public LocalDate dateValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public LocalTime timeValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public LocalTime timeValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public LocalDateTime datetimeValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public LocalDateTime datetimeValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public Instant timestampValue(String columnName) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public Instant timestampValue(int columnIndex) {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        public Iterator<Object> iterator() {
            return values.iterator();
        }
    }
}
