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

package org.apache.ignite.jdbc;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.ignite.internal.jdbc.ConnectionProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test checks the client time zone propagation from the jdbc client to the server node.
 */
@SuppressWarnings("CallToDriverManagerGetConnection")
public class ItJdbcClientTimeZoneTest extends AbstractJdbcSelfTest {
    private static final String TIMESTAMP_STR = "1970-01-01 00:00:00";

    private ZoneId origin;

    @BeforeAll
    static void createTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test(id INT PRIMARY KEY, ts TIMESTAMP, ts_tz TIMESTAMP WITH LOCAL TIME ZONE)");
        }
    }

    @BeforeEach
    void saveTimeZoneAndClearTable() throws SQLException {
        origin = ZoneId.systemDefault();

        stmt.execute("DELETE FROM test");
    }

    @AfterEach
    void restoreTimezone() {
        ZoneId current = ZoneId.systemDefault();

        if (!Objects.equals(origin, current)) {
            TimeZone.setDefault(TimeZone.getTimeZone(origin));
        }
    }

    /** Ensures that the default JVM time zone is passed to the server. */
    @Test
    public void jvmTimeZonePassedToServer() throws SQLException {
        ZoneId serverTimezone = TimeZone.getTimeZone("GMT+1").toZoneId();

        // Client time zone.
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+02:00"));

        withNewConnection(URL, stmt -> {
            // Set server timezone.
            TimeZone.setDefault(TimeZone.getTimeZone(serverTimezone));

            stmt.executeUpdate(format("INSERT INTO test VALUES(0, '{}', '{}')", TIMESTAMP_STR, TIMESTAMP_STR));

            validateSingleRow("SELECT ts::VARCHAR, ts_tz::VARCHAR FROM test", stmt,
                    "1970-01-01 00:00:00", "1970-01-01 00:00:00 GMT+02:00");
        });

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+03:00"));

        withNewConnection(URL, stmt -> {
            // Set server timezone.
            TimeZone.setDefault(TimeZone.getTimeZone(serverTimezone));

            validateSingleRow("SELECT ts::VARCHAR, ts_tz::VARCHAR FROM test", stmt,
                    "1970-01-01 00:00:00", "1970-01-01 01:00:00 GMT+03:00");
        });
    }

    /**
     * Ensures that session time zone can be changed using
     * connection property {@link ConnectionProperties#setConnectionTimeZone(ZoneId)}.
     */
    @Test
    public void timeZoneCanBeSetUsingProperty() throws SQLException {
        String originTimeZone = TimeZone.getDefault().getID();

        {
            String timeZone = "GMT+02:00";

            withNewConnection(URL + "?connectionTimeZone=" + timeZone, stmt -> {
                stmt.executeUpdate(format("INSERT INTO test VALUES(0, '{}', '{}')", TIMESTAMP_STR, TIMESTAMP_STR));

                validateSingleRow("SELECT ts::VARCHAR, ts_tz::VARCHAR FROM test", stmt,
                        "1970-01-01 00:00:00", "1970-01-01 00:00:00 " + timeZone);
            });
        }

        {
            String timeZone = "GMT+03:00";

            withNewConnection(URL + "?connectionTimeZone=" + timeZone, stmt -> {
                validateSingleRow("SELECT ts::VARCHAR, ts_tz::VARCHAR FROM test", stmt,
                        "1970-01-01 00:00:00", "1970-01-01 01:00:00 " + timeZone);
            });
        }

        {
            String timeZone = "invalid/timezone";

            SQLException ex = assertThrows(SQLException.class,
                    () -> DriverManager.getConnection(URL + "?connectionTimeZone=" + timeZone));

            assertThat(ex.getCause(), instanceOf(ZoneRulesException.class));
        }

        assertEquals(TimeZone.getDefault().getID(), originTimeZone);
    }

    /** Ensures that the value passed using a dynamic parameter respects the client's time zone. */
    @Test
    public void dynamicParamRespectsTimeZone() throws SQLException {
        // Client time zone.
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        Timestamp ts = timestamp("1970-01-01T00:00:00");

        // Session time zone is "GMT+1".
        try (Connection conn = DriverManager.getConnection(URL + "?connectionTimeZone=GMT+1")) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test VALUES(?, ?, ?)")) {
                stmt.setInt(1, 1);
                stmt.setTimestamp(2, ts);
                // The UTC value must be adjusted according to
                // session time zone and must be "1969-12-31 23:00:00 UTC".
                stmt.setTimestamp(3, ts);

                stmt.executeUpdate();
            }

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT ts, ts_tz FROM test where id=1")) {
                    assertTrue(rs.next());

                    // Session time zone was "GMT+1".
                    // Client time zone is "GMT".
                    {
                        assertEquals(timestamp("1970-01-01T00:00:00"), rs.getTimestamp(1));
                        // Since client time zone is GMT, timestamp will contain original UTC value from store.
                        assertEquals(timestamp("1969-12-31T23:00:00"), rs.getTimestamp(2));
                    }

                    // Session and client time zone are same ("GMT+1").
                    {
                        TimeZone.setDefault(TimeZone.getTimeZone("GMT+1"));

                        assertEquals(timestamp("1970-01-01T00:00:00"), rs.getTimestamp(1));
                        assertEquals(timestamp("1970-01-01T00:00:00"), rs.getTimestamp(2));
                    }

                    // Session time zone was "GMT+1".
                    // Client time zone is "GMT+2".
                    {
                        TimeZone.setDefault(TimeZone.getTimeZone("GMT+2"));

                        assertEquals(timestamp("1970-01-01T00:00:00"), rs.getTimestamp(1));
                        assertEquals(timestamp("1970-01-01T01:00:00"), rs.getTimestamp(2));
                    }
                }
            }
        }
    }

    private static Timestamp timestamp(String dateTimeString) {
        return Timestamp.valueOf(LocalDateTime.parse(dateTimeString));
    }

    private static void withNewConnection(String url, ConsumerX<Statement> consumer) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                consumer.accept(stmt);
            }
        }
    }

    private static void validateSingleRow(String query, Statement stmt, Object ... expected) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(query)) {
            assertTrue(rs.next());

            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], rs.getObject(i + 1));
            }

            assertFalse(rs.next());
        }
    }

    @FunctionalInterface
    private interface ConsumerX<T> {
        void accept(T obj) throws SQLException;
    }
}
