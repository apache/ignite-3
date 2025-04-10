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

package org.apache.ignite.migrationtools.tests.e2e.framework.runners;

import static org.junit.jupiter.api.Named.named;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.DiscoveryUtils;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.SqlTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** JDBCTestBootstrap. */
public class JdbcTestBootstrap {

    private static int N_TEST_EXAMPLES = 2_500;

    private Connection conn;

    private String jdbcUrl;

    /** Constructor. */
    public JdbcTestBootstrap() {
        // TODO: Log an warning if it goes to default?
        this(Optional.ofNullable(System.getProperty("jdbcURL"))
                .or(() -> Optional.ofNullable(System.getenv("JDBC_URL")))
                .orElse("jdbc:ignite:thin://127.0.0.1:10800"));
    }

    public JdbcTestBootstrap(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    @BeforeAll
    public static void setNumberOfSamples() {
        var numSamples = System.getenv("N_TEST_SAMPLES");
        if (numSamples != null) {
            N_TEST_EXAMPLES = Integer.parseUnsignedInt(numSamples);
        }
    }

    /** Arguments provider. */
    public static Stream<Arguments> provideTestArgs() {
        return DiscoveryUtils.discoverClasses().stream()
                .flatMap(tc -> {
                    Map<String, SqlTest> tests = tc.jdbcTests();
                    return tests.entrySet().stream()
                            .map(me -> {
                                String name = String.format("[%s] - %s; %s", tc.getClass().getSimpleName(), tc.getTableName(), me.getKey());
                                return Arguments.of(named(name, me.getValue()));
                            });
                });
    }

    @BeforeEach
    public void setupClient() throws ClassNotFoundException, SQLException {
        // Open the JDBC connection.
        this.conn = DriverManager.getConnection(this.jdbcUrl);
    }

    @AfterEach
    public void tearDownClient() throws SQLException {
        if (this.conn != null) {
            this.conn.close();
            this.conn = null;
        }
    }

    @DisplayName("JDBC API SQL Tests")
    @ParameterizedTest
    @MethodSource("provideTestArgs")
    public void runTest(SqlTest sqlTest) throws SQLException {
        sqlTest.test(conn, N_TEST_EXAMPLES);
    }
}
