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

import static org.apache.ignite.internal.jdbc.JdbcUtils.createObjectListResultSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlRow;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsException;

/**
 * Runs JdbcResultSetCompatibilityBaseTest against modern org.apache.ignite.internal.jdbc2.JdbcResultSet.
 */
public class JdbcResultSet2SelfTest extends JdbcResultSetBaseSelfTest {

    @Override
    @ParameterizedTest
    @EnumSource(names = {"PERIOD", "DURATION"}, mode = EnumSource.Mode.EXCLUDE)
    public void wasNullPositional(ColumnType columnType) throws SQLException {
        super.wasNullPositional(columnType);
    }

    @Override
    @ParameterizedTest
    @EnumSource(names = {"PERIOD", "DURATION"}, mode = EnumSource.Mode.EXCLUDE)
    public void wasNullNamed(ColumnType columnType) throws SQLException {
        super.wasNullNamed(columnType);
    }

    @Test
    public void unwrap() throws SQLException {
        try (ResultSet rs = createResultSet(null,
                List.of(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false)),
                List.of(List.of(true)))
        ) {
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
        try (ResultSet rs = createResultSet(null,
                List.of(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false)),
                List.of(List.of(true)))
        ) {
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals(1, metaData.getColumnCount());
        }

        // Empty metadata
        try (ResultSet rs = createResultSet(null, List.of(), List.of())) {
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals(0, metaData.getColumnCount());
        }
    }

    @Test
    public void nextExceptionIsWrapped() {
        // ClientResultSet hasNext() throws
        {
            Statement statement = Mockito.mock(Statement.class);

            ClientSyncResultSet clientRs = Mockito.mock(ClientSyncResultSet.class);
            when(clientRs.metadata()).thenReturn(ClientSyncResultSet.EMPTY_METADATA);

            RuntimeException cause = new RuntimeException("Some error");
            when(clientRs.hasNext()).thenThrow(cause);

            ResultSet rs = new JdbcResultSet(clientRs, statement, ZoneId::systemDefault, false, 0);

            SQLException err = assertThrows(SQLException.class, rs::next);
            assertEquals("Some error", err.getMessage());
            assertInstanceOf(IgniteException.class, err.getCause());
            assertSame(cause, err.getCause().getCause());
        }

        // ClientResultSet next() throws
        {
            Statement statement = Mockito.mock(Statement.class);

            ClientSyncResultSet clientRs = Mockito.mock(ClientSyncResultSet.class);
            when(clientRs.metadata()).thenReturn(ClientSyncResultSet.EMPTY_METADATA);

            RuntimeException cause = new RuntimeException("Some error");
            when(clientRs.hasNext()).thenReturn(true);
            when(clientRs.next()).thenThrow(cause);

            ResultSet rs = new JdbcResultSet(clientRs, statement, ZoneId::systemDefault, false, 0);

            SQLException err = assertThrows(SQLException.class, rs::next);
            assertEquals("Some error", err.getMessage());
            assertInstanceOf(IgniteException.class, err.getCause());
            assertSame(cause, err.getCause().getCause());
        }
    }

    @Test
    public void closeClosesResultSet() throws SQLException {
        Statement statement = Mockito.mock(Statement.class);

        ClientSyncResultSet clientRs = Mockito.mock(ClientSyncResultSet.class);
        when(clientRs.metadata()).thenReturn(ClientSyncResultSet.EMPTY_METADATA);

        JdbcStatement statement2 = Mockito.mock(JdbcStatement.class);
        when(statement.unwrap(JdbcStatement.class)).thenReturn(statement2);

        ResultSet rs = new JdbcResultSet(clientRs, statement, ZoneId::systemDefault, true, 0);

        rs.close();
        rs.close();

        verify(clientRs, times(1)).close();
        verify(clientRs, times(1)).metadata();
        verify(statement2, times(1)).closeIfAllResultsClosed();
        verify(clientRs, times(1)).hasNextResultSet();
        verifyNoMoreInteractions(clientRs, statement2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void closeExceptionIsWrapped(boolean closeOnCompletion) throws SQLException {
        JdbcStatement statement = Mockito.mock(JdbcStatement.class);
        when(statement.unwrap(JdbcStatement.class)).thenReturn(statement);

        ClientSyncResultSet clientRs = Mockito.mock(ClientSyncResultSet.class);
        when(clientRs.metadata()).thenReturn(ClientSyncResultSet.EMPTY_METADATA);

        RuntimeException cause = new RuntimeException("Some error");
        doAnswer(new ThrowsException(cause)).when(clientRs).close();

        ResultSet rs = new JdbcResultSet(clientRs, statement, ZoneId::systemDefault, closeOnCompletion, 0);

        SQLException err = assertThrows(SQLException.class, rs::close);
        assertEquals("Some error", err.getMessage());
        assertInstanceOf(IgniteException.class, err.getCause());
        assertSame(cause, err.getCause().getCause());
    }

    @Test
    public void getValueExceptionIsWrapped() throws SQLException {
        Statement statement = Mockito.mock(Statement.class);

        ClientSyncResultSet clientRs = Mockito.mock(ClientSyncResultSet.class);
        SqlRow row = Mockito.mock(SqlRow.class);

        ColumnMetadataImpl column = new ColumnMetadataImpl("C", ColumnType.INT32, 0, 0, false, null);

        when(clientRs.metadata()).thenReturn(new ResultSetMetadataImpl(List.of(column)));
        when(clientRs.hasNext()).thenReturn(true);
        when(clientRs.next()).thenReturn(row);

        RuntimeException cause = new RuntimeException("Corrupted value");
        when(row.value(0)).thenThrow(cause);

        JdbcResultSet rs = new JdbcResultSet(clientRs, statement, ZoneId::systemDefault, false, 0);
        assertTrue(rs.next());

        SQLException err = assertThrows(SQLException.class, () -> rs.getValue(1));
        assertEquals("Unable to value for column: 1", err.getMessage());
        assertInstanceOf(IgniteException.class, err.getCause());
        assertSame(cause, err.getCause().getCause());
    }

    @Test
    public void maxRows() throws SQLException {
        ColumnMetadataImpl column = new ColumnMetadataImpl("C1", ColumnType.INT32, 0, 0, false, null);
        List<ColumnMetadata> meta = List.of(column);
        Supplier<ZoneId> zoneIdSupplier = ZoneId::systemDefault;

        List<List<Object>> rows = List.of(
                List.of(1),
                List.of(2),
                List.of(3),
                List.of(4)
        );

        try (ResultSet rs = createObjectListResultSet(rows, meta, zoneIdSupplier, 3)) {
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());
            // MaxRows exceeded
            assertFalse(rs.next());
            assertFalse(rs.next());
        }

        // no limit

        try (ResultSet rs = createObjectListResultSet(rows, meta, zoneIdSupplier, 0)) {
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());
            // No more rows
            assertFalse(rs.next());
        }
    }

    @Override
    protected ResultSet createResultSet(@Nullable ZoneId zoneId, List<ColumnDefinition> cols, List<List<Object>> rows) {
        ZoneId timeZone = zoneId == null ? ZoneId.systemDefault() : zoneId;

        List<ColumnMetadata> apiCols = new ArrayList<>();
        for (ColumnDefinition c : cols) {
            String schema = c.schema;
            String table = c.table;
            String column = c.column != null ? c.column : c.label.toUpperCase(Locale.US);
            boolean nullable = true;
            ColumnOriginImpl origin = new ColumnOriginImpl(schema, table, column);
            apiCols.add(new ColumnMetadataImpl(c.label, c.type, c.precision, c.scale, nullable, origin));
        }

        return createObjectListResultSet(rows, apiCols, () -> timeZone, 0);
    }
}
