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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.jdbc.util.JdbcTestUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ItJdbcAuthenticationTest {
    @Nested
    @DisplayName("Given basic authentication disabled on the cluster")
    class ClusterWithoutAuth extends ClusterPerClassIntegrationTest {
        @Override
        protected int initialNodes() {
            return 1;
        }

        @Test
        @DisplayName("Jdbc client can connect without basic authentication configured")
        void jdbcCanConnectWithoutBasicAuth() throws SQLException {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            try (Connection ignored = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }

    @Nested
    @DisplayName("Given basic authentication enabled on the cluster")
    class ClusterWithAuth extends ClusterPerClassIntegrationTest {
        @Override
        protected int initialNodes() {
            return 1;
        }

        @Override
        protected void configureInitParameters(InitParametersBuilder builder) {
            builder.clusterConfiguration(
                    "ignite {\n"
                            + "  \"security\": {\n"
                            + "  \"enabled\": true,\n"
                            + "    \"authentication\": {\n"
                            + "      \"providers\": [\n"
                            + "        {\n"
                            + "          \"name\": \"default\",\n"
                            + "          \"type\": \"basic\",\n"
                            + "          \"users\": [\n"
                            + "            {\n"
                            + "              \"username\": \"usr\",\n"
                            + "              \"password\": \"pwd\"\n"
                            + "            }\n"
                            + "          ]\n"
                            + "        }\n"
                            + "      ]\n"
                            + "    }\n"
                            + "  }\n"
                            + "}\n"
            );
        }

        @Test
        @DisplayName("Jdbc client can not connect without basic authentication configured")
        void jdbcCanNotConnectWithoutBasicAuthentication() {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            JdbcTestUtils.assertThrowsSqlException("Failed to connect to server", () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Jdbc client can connect with basic authentication configured")
        void jdbcCanConnectWithBasicAuthentication() throws SQLException {
            var url = "jdbc:ignite:thin://127.0.0.1:10800"
                    + "?username=usr"
                    + "&password=pwd";
            try (Connection ignored = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }
}
