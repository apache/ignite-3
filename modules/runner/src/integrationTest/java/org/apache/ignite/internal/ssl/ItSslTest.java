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

package org.apache.ignite.internal.ssl;

import static org.apache.ignite.client.ClientAuthenticationMode.REQUIRE;
import static org.apache.ignite.internal.rest.RestNode.getResourcePath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.SslConfiguration;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** SSL support integration test. */
public class ItSslTest extends IgniteIntegrationTest {

    private static String password;

    private static String trustStorePath;

    private static String keyStorePath;

    @BeforeAll
    static void beforeAll() {
        password = "changeit";
        trustStorePath = getResourcePath(ItSslTest.class.getClassLoader().getResource("ssl/truststore.jks"));
        keyStorePath = getResourcePath(ItSslTest.class.getClassLoader().getResource("ssl/keystore.p12"));
    }

    @Nested
    @DisplayName("Given SSL disabled on the cluster")
    class ClusterWithoutSsl {

        @Language("JSON")
        String sslDisabledBoostrapConfig = "{\n"
                + "  network: {\n"
                + "    ssl.enabled: false,\n"
                + "    port: 3355,\n"
                + "    portRange: 2,\n"
                + "    nodeFinder:{\n"
                + "      netClusterNodes: [ \"localhost:3355\", \"localhost:3356\" ]\n"
                + "    }\n"
                + "  }\n"
                + "}";

        @WorkDirectory
        private Path workDir;
        private Cluster cluster;

        @BeforeEach
        void setUp(TestInfo testInfo) {
            cluster = new Cluster(testInfo, workDir, sslDisabledBoostrapConfig);
            cluster.startAndInit(2);
        }

        @AfterEach
        void tearDown() {
            cluster.shutdown();
        }

        @Test
        @DisplayName("SSL disabled and cluster starts")
        void clusterStartsWithDisabledSsl(TestInfo testInfo) {
            assertThat(cluster.runningNodes().count(), is(2L));
        }

        @Test
        @DisplayName("Client can connect without ssl")
        void clientCouldConnectWithoutSsl() throws Exception {
            try (IgniteClient client = IgniteClient.builder().addresses("localhost:10800").build()) {
                assertThat(client.clusterNodes(), hasSize(2));
            }
        }

        @Test
        @DisplayName("Client can not connect with ssl configured when ssl disabled on the cluster")
        void clientCanNotConnectWithSsl() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStoreType("JKS")
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(password)
                            .build();

            assertThrows(
                    IgniteClientConnectionException.class,
                    () -> IgniteClient.builder()
                            .addresses("localhost:10800")
                            .ssl(sslConfiguration)
                            .build()
            );
        }

        @Test
        @DisplayName("Jdbc driver could establish the connection when SSL disabled")
        void jdbcCouldConnectWithoutSsl() throws SQLException {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            try (Connection conn = DriverManager.getConnection(url)) {
                // No-op.
            }
        }

        @Test
        @DisplayName("Jdbc client can not connect with SSL configured when SSL disabled on the server")
        void jdbcCanNotConnectWithSsl() {
            var url =
                    "jdbc:ignite:thin://127.0.0.1:10800"
                            + "?sslEnabled=true"
                            + "&trustStorePath=" + trustStorePath
                            + "&trustStoreType=JKS"
                            + "&trustStorePassword=" + password;
            assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        }
    }

    @Nested
    @DisplayName("Given SSL enabled on the cluster")
    class ClusterWithSsl {

        @Language("JSON")
        String sslEnabledBoostrapConfig = "{\n"
                + "  network: {\n"
                + "    ssl : {"
                + "      enabled: true,\n"
                + "      trustStore: {\n"
                + "        password: \"" + password + "\","
                + "        path: \"" + trustStorePath + "\""
                + "      },\n"
                + "      keyStore: {\n"
                + "        password: \"" + password + "\","
                + "        path: \"" + keyStorePath + "\""
                + "      }\n"
                + "    },\n"
                + "    port: 3345,\n"
                + "    portRange: 2,\n"
                + "    nodeFinder:{\n"
                + "      netClusterNodes: [ \"localhost:3345\", \"localhost:3346\" ]\n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector.ssl: {\n"
                + "    enabled: true, "
                + "    keyStore: {\n"
                + "      path: \"" + keyStorePath + "\",\n"
                + "      password: \"" + password + "\"\n"
                + "    }\n"
                + "  }\n"
                + "}";

        @WorkDirectory
        private Path workDir;
        private Cluster cluster;

        @BeforeEach
        void setUp(TestInfo testInfo) {
            cluster = new Cluster(testInfo, workDir, sslEnabledBoostrapConfig);
            cluster.startAndInit(2);
        }

        @AfterEach
        void tearDown() {
            cluster.shutdown();
        }

        @Test
        @DisplayName("SSL enabled and setup correctly then cluster starts")
        void clusterStartsWithEnabledSsl(TestInfo testInfo) {
            assertThat(cluster.runningNodes().count(), is(2L));
        }

        @Test
        @DisplayName("Client cannot connect without SSL configured")
        void clientCannotConnectWithoutSsl() {
            assertThrows(IgniteClientConnectionException.class, () -> {
                try (IgniteClient ignored = IgniteClient.builder().addresses("localhost:10800").build()) {
                    // no-op
                }
            });
        }

        @Test
        void jdbcCannotConnectWithoutSsl() {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Client can connect with SSL configured")
        void clientCanConnectWithSsl() throws Exception {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStoreType("JKS")
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(password)
                            .build();

            try (IgniteClient client = IgniteClient.builder()
                    .addresses("localhost:10800")
                    .ssl(sslConfiguration)
                    .build()
            ) {
                assertThat(client.clusterNodes(), hasSize(2));
            }
        }

        @Test
        @DisplayName("Jdbc client can connect with SSL configured")
        void jdbcCanConnectWithSsl() throws SQLException {
            var url =
                    "jdbc:ignite:thin://127.0.0.1:10800"
                            + "?sslEnabled=true"
                            + "&trustStorePath=" + trustStorePath
                            + "&trustStoreType=JKS"
                            + "&trustStorePassword=" + password;
            try (Connection conn = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }

    @Nested
    @DisplayName("Given SSL enabled on the cluster and specific cipher enabled")
    class ClusterWithSslCustomCipher {

        @Language("JSON")
        String sslEnabledWithCipherBoostrapConfig = "{\n"
                + "  network: {\n"
                + "    ssl : {"
                + "      enabled: true,\n"
                + "      ciphers: TLS_AES_256_GCM_SHA384,\n"
                + "      trustStore: {\n"
                + "        password: \"" + password + "\","
                + "        path: \"" + trustStorePath + "\""
                + "      },\n"
                + "      keyStore: {\n"
                + "        password: \"" + password + "\","
                + "        path: \"" + keyStorePath + "\""
                + "      }\n"
                + "    },\n"
                + "    port: 3345,\n"
                + "    portRange: 2,\n"
                + "    nodeFinder:{\n"
                + "      netClusterNodes: [ \"localhost:3345\", \"localhost:3346\" ]\n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector.ssl: {\n"
                + "    enabled: true, "
                + "    keyStore: {\n"
                + "      path: \"" + keyStorePath + "\",\n"
                + "      password: \"" + password + "\"\n"
                + "    }\n"
                + "  }\n"
                + "}";

        @WorkDirectory
        private Path workDir;
        private Cluster cluster;

        @BeforeEach
        void setUp(TestInfo testInfo) {
            cluster = new Cluster(testInfo, workDir, sslEnabledWithCipherBoostrapConfig);
            cluster.startAndInit(2);
        }

        @AfterEach
        void tearDown() {
            cluster.shutdown();
        }

        @Test
        @DisplayName("SSL enabled and setup correctly then cluster starts")
        void clusterStartsWithEnabledSsl(TestInfo testInfo) {
            assertThat(cluster.runningNodes().count(), is(2L));
        }

        @Test
        @DisplayName("Client cannot connect without SSL configured")
        void clientCannotConnectWithoutSsl() {
            assertThrows(IgniteClientConnectionException.class, () -> {
                try (IgniteClient ignored = IgniteClient.builder().addresses("localhost:10800").build()) {
                    // no-op
                } catch (IgniteClientConnectionException e) {
                    throw e;
                }
            });
        }

        @Test
        void jdbcCannotConnectWithoutSsl() {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Client can connect with SSL configured")
        void clientCanConnectWithSsl() throws Exception {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStoreType("JKS")
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(password)
                            .build();

            try (IgniteClient client = IgniteClient.builder()
                    .addresses("localhost:10800")
                    .ssl(sslConfiguration)
                    .build()
            ) {
                assertThat(client.clusterNodes(), hasSize(2));
            }
        }

        @Test
        @DisplayName("Client cannot connect with SSL configured with non-matcher cipher")
        void clientCannotConnectWithSslAndWrongCipher() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .ciphers(List.of("TLS_DHE_RSA_WITH_AES_128_CBC_SHA"))
                            .trustStoreType("JKS")
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(password)
                            .build();

            assertThrows(IgniteClientConnectionException.class, () -> {
                try (IgniteClient ignored = IgniteClient.builder()
                        .addresses("localhost:10800")
                        .ssl(sslConfiguration)
                        .build()) {
                    // no-op
                }
            });
        }

        @Test
        void jdbcCannotConnectWithSslAndWrongCipher() {
            var url =
                    "jdbc:ignite:thin://127.0.0.1:10800"
                            + "?sslEnabled=true"
                            + "&ciphers=TLS_DHE_RSA_WITH_AES_128_CBC_SHA"
                            + "&trustStorePath=" + trustStorePath
                            + "&trustStoreType=JKS"
                            + "&trustStorePassword=" + password;
            assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Jdbc client can connect with SSL configured")
        void jdbcCanConnectWithSsl() throws SQLException {
            var url =
                    "jdbc:ignite:thin://127.0.0.1:10800"
                            + "?sslEnabled=true"
                            + "&trustStorePath=" + trustStorePath
                            + "&trustStoreType=JKS"
                            + "&trustStorePassword=" + password;
            try (Connection conn = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }

    @Nested
    @DisplayName("Given SSL enabled client auth is set to require on the cluster")
    class ClusterWithSslAndClientAuth {

        @Language("JSON")
        String sslEnabledBoostrapConfig = "{\n"
                + "  network: {\n"
                + "    ssl : {"
                + "      enabled: true,\n"
                + "      clientAuth: \"require\",\n"
                + "      trustStore: {\n"
                + "        password: \"" + password + "\","
                + "        path: \"" + trustStorePath + "\""
                + "      },\n"
                + "      keyStore: {\n"
                + "        password: \"" + password + "\","
                + "        path: \"" + keyStorePath + "\""
                + "      }\n"
                + "    },\n"
                + "    port: 3365,\n"
                + "    portRange: 2,\n"
                + "    nodeFinder:{\n"
                + "      netClusterNodes: [ \"localhost:3365\", \"localhost:3366\" ]\n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector.ssl: {\n"
                + "    enabled: true, "
                + "    clientAuth: \"require\", "
                + "    keyStore: {\n"
                + "      path: \"" + keyStorePath + "\",\n"
                + "      password: \"" + password + "\"\n"
                + "    }, \n"
                + "    trustStore: {\n"
                + "      type: JKS,"
                + "      password: \"" + password + "\","
                + "      path: \"" + trustStorePath + "\""
                + "      }\n"
                + "  }\n"
                + "}";

        @WorkDirectory
        private Path workDir;
        private Cluster cluster;

        @BeforeEach
        void setUp(TestInfo testInfo) {
            cluster = new Cluster(testInfo, workDir, sslEnabledBoostrapConfig);
            cluster.startAndInit(2);
        }

        @AfterEach
        void tearDown() {
            cluster.shutdown();
        }

        @Test
        @DisplayName("SSL enabled and setup correctly then cluster starts")
        void clusterStartsWithEnabledSsl(TestInfo testInfo) {
            assertThat(cluster.runningNodes().count(), is(2L));
        }

        @Test
        @DisplayName("Client cannot connect without SSL configured")
        void clientCannotConnectWithoutSsl() {
            assertThrows(IgniteClientConnectionException.class, () -> {
                try (IgniteClient ignored = IgniteClient.builder().addresses("localhost:10800").build()) {
                    // no-op
                }
            });
        }

        @Test
        void jdbcCannotConnectWithoutSsl() {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Client can not connect without client authentication configured")
        void clientCanNotConnectWithoutClientAuth() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStoreType("JKS")
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(password)
                            .build();

            assertThrows(IgniteClientConnectionException.class,
                    () -> IgniteClient.builder()
                            .addresses("localhost:10800")
                            .ssl(sslConfiguration)
                            .build()
            );
        }

        @Test
        @DisplayName("Client can connect with SSL and client authentication configured")
        void clientCanConnectWithSslAndClientAuth() throws Exception {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStoreType("JKS")
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(password)
                            .clientAuth(REQUIRE)
                            .keyStorePath(keyStorePath)
                            .keyStorePassword(password)
                            .build();

            try (IgniteClient client = IgniteClient.builder()
                    .addresses("localhost:10800")
                    .ssl(sslConfiguration)
                    .build()
            ) {
                assertThat(client.clusterNodes(), hasSize(2));
            }
        }

        @Test
        @DisplayName("Jdbc client can connect with SSL configured")
        void jdbcCanConnectWithSslAndClientAuth() throws SQLException {
            var url =
                    "jdbc:ignite:thin://127.0.0.1:10800"
                            + "?sslEnabled=true"
                            + "&trustStorePath=" + trustStorePath
                            + "&trustStoreType=JKS"
                            + "&trustStorePassword=" + password
                            + "&clientAuth=require"
                            + "&keyStorePath=" + keyStorePath
                            + "&keyStoreType=PKCS12"
                            + "&keyStorePassword=" + password;
            try (Connection conn = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }
}
