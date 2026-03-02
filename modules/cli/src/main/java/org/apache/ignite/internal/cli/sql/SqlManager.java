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

package org.apache.ignite.internal.cli.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringJoiner;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.cli.sql.SqlQueryResult.SqlQueryResultBuilder;
import org.apache.ignite.internal.cli.sql.table.Table;

/**
 * Manager to work with any sql operation.
 */
public class SqlManager implements AutoCloseable {
    private final String jdbcUrl;

    private final Connection connection;

    private boolean connectionInfoLogged;

    /**
     * Default constructor.
     */
    public SqlManager(String jdbcUrl) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        connection = DriverManager.getConnection(jdbcUrl);
    }

    /**
     * Execute provided string as SQL request.
     *
     * @param sql incoming string representation of SQL command.
     * @return result of provided SQL command in terms of {@link Table}.
     * @throws SQLException in any case when SQL command can't be executed.
     */
    public SqlQueryResult execute(String sql) throws SQLException {
        logConnectionInfo();
        CliLoggers.verboseLog(1, "--> SQL " + sql);

        SqlQueryResultBuilder sqlQueryResultBuilder = new SqlQueryResultBuilder();

        long startTime = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);

            int totalRows = 0;
            do {
                ResultSet rs = statement.getResultSet();
                if (rs != null) {
                    logColumnMetadata(rs.getMetaData());
                    Table<String> table = Table.fromResultSet(rs);
                    totalRows += table.content().length;
                    sqlQueryResultBuilder.addTable(table);
                } else {
                    int updateCount = statement.getUpdateCount();
                    sqlQueryResultBuilder.addMessage(updateCount >= 0 ? "Updated " + updateCount + " rows." : "OK!");
                }
            } while (statement.getMoreResults() || statement.getUpdateCount() != -1);

            long durationMs = System.currentTimeMillis() - startTime;
            sqlQueryResultBuilder.setDurationMs(durationMs);
            CliLoggers.verboseLog(1, "<-- " + totalRows + " row(s) (" + durationMs + "ms)");
            return sqlQueryResultBuilder.build();
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    public DatabaseMetaData getMetadata() throws SQLException {
        return connection.getMetaData();
    }

    private void logConnectionInfo() throws SQLException {
        if (connectionInfoLogged) {
            return;
        }
        connectionInfoLogged = true;

        CliLoggers.verboseLog(1, "--> JDBC " + jdbcUrl);

        if (CliLoggers.getVerboseLevel() >= 3) {
            DatabaseMetaData meta = connection.getMetaData();
            CliLoggers.verboseLog(3, "--> Driver: " + meta.getDriverName() + " " + meta.getDriverVersion());
        }
    }

    private static void logColumnMetadata(ResultSetMetaData metaData) throws SQLException {
        if (CliLoggers.getVerboseLevel() < 2) {
            return;
        }

        int columnCount = metaData.getColumnCount();
        StringJoiner joiner = new StringJoiner(", ");
        for (int i = 1; i <= columnCount; i++) {
            joiner.add(metaData.getColumnLabel(i) + " (" + metaData.getColumnTypeName(i) + ")");
        }
        CliLoggers.verboseLog(2, "<-- Columns: " + joiner);
    }
}
