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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.security.AuthenticationConfig;
import org.apache.ignite.security.BasicAuthenticationProviderConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

class ItJdbcTest extends IgniteIntegrationTest {
    private Cluster cluster;

    @Nested
    @DisplayName("Given basic auth disabled on the cluster")
    @TestInstance(Lifecycle.PER_CLASS)
    class ClusterWithoutAuth {
        @BeforeAll
        void setUp(TestInfo testInfo, @WorkDirectory Path workDir) {
            cluster = new Cluster(testInfo, workDir);
            cluster.startAndInit(1);
        }

        @AfterAll
        void tearDown() {
            cluster.shutdown();
        }

        @Test
        @DisplayName("Jdbc client can connect without basic auth configured")
        void jdbcCanConnectWithoutBasicAuth() throws SQLException {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            try (Connection ignored = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }

    @Nested
    @DisplayName("Given basic auth enabled on the cluster")
    @TestInstance(Lifecycle.PER_CLASS)
    class ClusterWithAuth {
        @BeforeAll
        void setUp(TestInfo testInfo, @WorkDirectory Path workDir) {
            cluster = new Cluster(testInfo, workDir);
            cluster.startAndInit(1, builder -> builder.authenticationConfig(new AuthenticationConfig(
                    true,
                    List.of(new BasicAuthenticationProviderConfig("basic", "usr", "pwd")))
            ));
        }

        @AfterAll
        void tearDown() {
            cluster.shutdown();
        }

        @Test
        @DisplayName("Jdbc client can not connect without basic auth configured")
        void jdbcCanNotConnectWithoutBasicAuth() {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Jdbc client can connect with basic auth configured")
        void jdbcCanConnectWithBasicAuth() throws SQLException {
            var url = "jdbc:ignite:thin://127.0.0.1:10800"
                    + "?basicAuthUsername=usr"
                    + "&basicAuthPassword=pwd";
            try (Connection ignored = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }
}
