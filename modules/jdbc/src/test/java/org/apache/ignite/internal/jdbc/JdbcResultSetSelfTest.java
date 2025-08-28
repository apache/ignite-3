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

import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

/**
 * Runs JdbcResultSetCompatibilityBaseTest against legacy org.apache.ignite.internal.jdbc.JdbcResultSet.
 */
public class JdbcResultSetSelfTest extends JdbcResultSetBaseSelfTest {
    @Override
    protected ResultSet createResultSet(@Nullable ZoneId zoneId, List<ColumnDefinition> cols, List<List<Object>> rows) throws SQLException {
        // Convert ColumnSpec to legacy JDBC metadata
        List<JdbcColumnMeta> jdbcCols = new ArrayList<>();
        for (ColumnDefinition c : cols) {
            boolean nullable = true;
            jdbcCols.add(new JdbcColumnMeta(c.label, c.schema, c.table, c.column, c.type, c.precision, c.scale, nullable));
        }

        JdbcStatement statement = Mockito.mock(JdbcStatement.class);

        if (zoneId != null) {
            JdbcConnection connection = Mockito.mock(JdbcConnection.class);

            ConnectionPropertiesImpl connectionProperties = new ConnectionPropertiesImpl();
            connectionProperties.setConnectionTimeZone(zoneId);

            when(statement.getConnection()).thenReturn(connection);
            when(connection.connectionProperties()).thenReturn(connectionProperties);
        }

        when(statement.getResultSetType()).thenReturn(ResultSet.TYPE_FORWARD_ONLY);

        try {
            return new JdbcResultSet(rows, jdbcCols, statement);
        } catch (SQLException e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }
}
