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

package org.apache.ignite.internal;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.sql.SqlRow;
import org.jetbrains.annotations.Nullable;

/**
 * Common utilities for compatibility tests.
 */
public class CompatibilityTestCommon {
    public static final String TABLE_NAME_TEST = "TEST";

    public static final String TABLE_NAME_ALL_COLUMNS = "ALL_COLUMNS";

    /**
     * Creates default tables for compatibility tests.
     *
     * @param client Ignite client instance.
     */
    public static void createDefaultTables(Ignite client) {
        if (!ddl(client, "CREATE TABLE IF NOT EXISTS " + TABLE_NAME_TEST + " (id INT PRIMARY KEY, name VARCHAR)")) {
            sql(client, "DELETE FROM " + TABLE_NAME_TEST);
        }

        sql(client, "INSERT INTO " + TABLE_NAME_TEST + " (id, name) VALUES (1, 'test')");

        if (!ddl(client, "CREATE TABLE IF NOT EXISTS " + TABLE_NAME_ALL_COLUMNS + " (id INT PRIMARY KEY, byte TINYINT, short SMALLINT, "
                + "int INT, long BIGINT, float REAL, double DOUBLE, dec DECIMAL(10,1), "
                + "string VARCHAR, guid UUID, dt DATE, tm TIME(9), ts TIMESTAMP(9), "
                + "tstz TIMESTAMP WITH LOCAL TIME ZONE, bool BOOLEAN, bytes VARBINARY)")) {
            sql(client, "DELETE FROM " + TABLE_NAME_ALL_COLUMNS);
        }

        sql(client, "INSERT INTO " + TABLE_NAME_ALL_COLUMNS + " (id, byte, short, int, long, float, double, dec, "
                + "string, guid, dt, tm, ts, tstz, bool, bytes) VALUES "
                + "(1, 1, 2, 3, 4, 5.0, 6.0, 7.0, 'test', '10000000-2000-3000-4000-500000000000'::UUID, "
                + "date '2023-01-01', time '12:00:00', timestamp '2023-01-01 12:00:00', "
                + "?, true, X'01020304')", Instant.ofEpochSecond(1714946523L));

    }

    /**
     * Executes a SQL query and returns the result as a list of {@link SqlRow}.
     *
     * @param client Ignite client instance.
     * @param sql SQL query to execute.
     * @param arguments Arguments for the SQL query.
     * @return List of {@link SqlRow} if the query returns a row set, otherwise null.
     */
    public static @Nullable List<SqlRow> sql(Ignite client, String sql, Object... arguments) {
        try (var cursor = client.sql().execute(null, sql, arguments)) {
            if (cursor.hasRowSet()) {
                List<SqlRow> rows = new ArrayList<>();
                cursor.forEachRemaining(rows::add);
                return rows;
            } else {
                return null;
            }
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean ddl(Ignite client, String sql) {
        try (var cursor = client.sql().execute(null, sql)) {
            return cursor.wasApplied();
        }
    }
}
