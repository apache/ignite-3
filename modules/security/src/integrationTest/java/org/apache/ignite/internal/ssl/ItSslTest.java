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

import static org.apache.ignite.internal.ConfigTemplates.NL;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getResourcePath;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.SslConfiguration;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.Cluster.ServerRegistration;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/** SSL support integration test. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItSslTest {

    private static final String PASSWORD = "changeit";

    private static String trustStorePath;

    private static String keyStorePath;

    @BeforeAll
    static void beforeAll() {
        trustStorePath = getResourcePath(ItSslTest.class, "ssl/truststore.jks");
        keyStorePath = getResourcePath(ItSslTest.class, "ssl/keystore.p12");
    }

    @Nested
    @DisplayName("Given SSL disabled on the cluster")
    class ClusterWithoutSsl extends ClusterPerClassIntegrationTest {
        @Language("JSON")
        String sslDisabledBoostrapConfig = "ignite {" + NL
                + "  network: {" + NL
                + "    ssl.enabled: false," + NL
                + "    port: {}," + NL
                + "    nodeFinder:{" + NL
                + "      netClusterNodes: [ {}, \"localhost:3355\", \"localhost:3356\" ]" + NL
                + "    }" + NL
                + "  }," + NL
                + "  clientConnector: { port: {} }," + NL
                + "  rest: {" + NL
                + "    port: {}," + NL
                + "    ssl.port: {}" + NL
                + "  }," + NL
                + "  failureHandler.handler.type: noop,"  + NL
                + "  failureHandler.dumpThreadsOnFailure: false" + NL
                + "}";

        @Override
        protected int initialNodes() {
            return 2;
        }

        @Override
        protected String getNodeBootstrapConfigTemplate() {
            return sslDisabledBoostrapConfig;
        }

        @Test
        @DisplayName("SSL disabled and cluster starts")
        void clusterStartsWithDisabledSsl(TestInfo testInfo) {
            assertThat(CLUSTER.runningNodes().count(), is(2L));
        }

        @Test
        @DisplayName("Client can connect without ssl")
        void clientCouldConnectWithoutSsl() {
            try (IgniteClient client = IgniteClient.builder().addresses("localhost:10800").build()) {
                assertThat(client.cluster().nodes(), hasSize(2));
            }
        }

        @Test
        @DisplayName("Client can not connect with ssl configured when ssl disabled on the cluster")
        void clientCanNotConnectWithSsl() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(PASSWORD)
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
                            + "&trustStorePassword=" + PASSWORD;
            assertThrowsSqlException(SQLException.class, () -> DriverManager.getConnection(url));
        }
    }

    @Nested
    @DisplayName("Given SSL enabled on the cluster")
    class ClusterWithSsl extends ClusterPerClassIntegrationTest {
        @Language("JSON")
        String sslEnabledBoostrapConfig = "ignite {" + NL
                + "  network: {" + NL
                + "    ssl : {"
                + "      enabled: true," + NL
                + "      trustStore: {" + NL
                + "        password: \"" + PASSWORD + "\","
                + "        path: \"" + escapeWindowsPath(trustStorePath) + "\""
                + "      }," + NL
                + "      keyStore: {" + NL
                + "        password: \"" + PASSWORD + "\","
                + "        path: \"" + escapeWindowsPath(keyStorePath) + "\""
                + "      }" + NL
                + "    }," + NL
                + "    port: {}," + NL
                + "    nodeFinder:{" + NL
                + "      netClusterNodes: [ {}, \"localhost:3345\", \"localhost:3346\" ]" + NL
                + "    }" + NL
                + "  }," + NL
                + "  clientConnector: { port: {} }," + NL
                + "  clientConnector.ssl: {" + NL
                + "    enabled: true, "
                + "    keyStore: {" + NL
                + "      path: \"" + escapeWindowsPath(keyStorePath) + "\"," + NL
                + "      password: \"" + PASSWORD + "\"" + NL
                + "    }" + NL
                + "  }," + NL
                + "  rest: {" + NL
                + "    port: {}," + NL
                + "    ssl.port: {}" + NL
                + "  }," + NL
                + "  failureHandler.handler.type: noop," + NL
                + "  failureHandler.dumpThreadsOnFailure: false" + NL
                + "}";

        @Override
        protected int initialNodes() {
            return 2;
        }

        @Override
        protected String getNodeBootstrapConfigTemplate() {
            return sslEnabledBoostrapConfig;
        }

        @Test
        @DisplayName("SSL enabled and setup correctly then cluster starts")
        void clusterStartsWithEnabledSsl(TestInfo testInfo) {
            assertThat(CLUSTER.runningNodes().count(), is(2L));
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
        @DisplayName("Client cannot connect with SSL configured and invalid trust store password")
        void clientCanNotConnectWithSslAndInvalidTrustStorePassword() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(PASSWORD + "_foo")
                            .build();

            IgniteClientConnectionException ex = assertThrows(IgniteClientConnectionException.class, () -> {
                try (IgniteClient ignored = IgniteClient.builder().addresses("localhost:10800").ssl(sslConfiguration).build()) {
                    // no-op
                }
            });

            assertThat(ex.getMessage(), is("Client SSL configuration error: keystore password was incorrect"));
            // Exceptions thrown from the synchronous build method are copied to include the sync method
            assertThat(ex.getCause(), isA(IgniteClientConnectionException.class));
            assertThat(ex.getCause().getCause(), isA(IOException.class));
            assertThat(ex.getCause().getCause().getMessage(), is("keystore password was incorrect"));
        }

        @Test
        @DisplayName("Client cannot connect with SSL configured and invalid trust store password async method")
        void clientCanNotConnectWithSslAndInvalidTrustStorePasswordAsync() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(PASSWORD + "_foo")
                            .build();

            CompletableFuture<IgniteClient> clientFuture =
                    IgniteClient.builder().addresses("localhost:10800").ssl(sslConfiguration).buildAsync();

            assertThat(clientFuture, willThrow(IgniteClientConnectionException.class));

            CompletionException completionException = assertThrows(CompletionException.class, clientFuture::join);

            assertThat(completionException.getCause(), isA(IgniteClientConnectionException.class));
            IgniteClientConnectionException ex = (IgniteClientConnectionException) completionException.getCause();

            assertThat(ex.getMessage(), is("Client SSL configuration error: keystore password was incorrect"));
            assertThat(ex.getCause(), isA(IOException.class));
            assertThat(ex.getCause().getMessage(), is("keystore password was incorrect"));
        }

        @Test
        void jdbcCannotConnectWithoutSsl() {
            var url = "jdbc:ignite:thin://127.0.0.1:10800";
            assertThrowsSqlException(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Client can connect with SSL configured")
        void clientCanConnectWithSsl() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(PASSWORD)
                            .build();

            try (IgniteClient client = IgniteClient.builder()
                    .addresses("localhost:10800")
                    .ssl(sslConfiguration)
                    .build()
            ) {
                assertThat(client.cluster().nodes(), hasSize(2));
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
                            + "&trustStorePassword=" + PASSWORD;
            try (Connection conn = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }

    @Nested
    @DisplayName("Given SSL enabled on the cluster and specific cipher enabled")
    class ClusterWithSslCustomCipher extends ClusterPerClassIntegrationTest {
        String sslEnabledWithCipherBoostrapConfig = createBoostrapConfig("TLS_AES_256_GCM_SHA384");

        @Override
        protected int initialNodes() {
            return 2;
        }

        @Override
        protected String getNodeBootstrapConfigTemplate() {
            return sslEnabledWithCipherBoostrapConfig;
        }

        @Test
        @DisplayName("SSL enabled and setup correctly then cluster starts")
        void clusterStartsWithEnabledSsl(TestInfo testInfo) {
            assertThat(CLUSTER.runningNodes().count(), is(2L));
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
            assertThrowsSqlException(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Client can connect with SSL configured")
        void clientCanConnectWithSsl() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(PASSWORD)
                            .build();

            try (IgniteClient client = IgniteClient.builder()
                    .addresses("localhost:10800")
                    .ssl(sslConfiguration)
                    .build()
            ) {
                assertThat(client.cluster().nodes(), hasSize(2));
            }
        }

        @Test
        @DisplayName("Client cannot connect with SSL configured with non-matcher cipher")
        void clientCannotConnectWithSslAndWrongCipher() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .ciphers(List.of("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"))
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(PASSWORD)
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
                            + "&ciphers=TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
                            + "&trustStorePath=" + trustStorePath
                            + "&trustStoreType=JKS"
                            + "&trustStorePassword=" + PASSWORD;
            assertThrowsSqlException(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Jdbc client can connect with SSL configured")
        void jdbcCanConnectWithSsl() throws SQLException {
            var url =
                    "jdbc:ignite:thin://127.0.0.1:10800"
                            + "?sslEnabled=true"
                            + "&trustStorePath=" + trustStorePath
                            + "&trustStoreType=JKS"
                            + "&trustStorePassword=" + PASSWORD;
            try (Connection conn = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }

    @Nested
    @DisplayName("Given SSL enabled client auth is set to require on the cluster")
    class ClusterWithSslAndClientAuth extends ClusterPerClassIntegrationTest {
        @Language("JSON")
        String sslEnabledBoostrapConfig = "ignite {" + NL
                + "  network: {" + NL
                + "    ssl : {"
                + "      enabled: true," + NL
                + "      clientAuth: \"require\"," + NL
                + "      trustStore: {" + NL
                + "        password: \"" + PASSWORD + "\","
                + "        path: \"" + escapeWindowsPath(trustStorePath) + "\""
                + "      }," + NL
                + "      keyStore: {" + NL
                + "        password: \"" + PASSWORD + "\","
                + "        path: \"" + escapeWindowsPath(keyStorePath) + "\""
                + "      }" + NL
                + "    }," + NL
                + "    port: {}," + NL
                + "    nodeFinder:{" + NL
                + "      netClusterNodes: [ {}, \"localhost:3365\", \"localhost:3366\" ]" + NL
                + "    }" + NL
                + "  }," + NL
                + "  clientConnector: { port: {} }," + NL
                + "  clientConnector.ssl: {" + NL
                + "    enabled: true, "
                + "    clientAuth: \"require\", "
                + "    keyStore: {" + NL
                + "      path: \"" + escapeWindowsPath(keyStorePath) + "\"," + NL
                + "      password: \"" + PASSWORD + "\"" + NL
                + "    }," + NL
                + "    trustStore: {" + NL
                + "      type: JKS,"
                + "      password: \"" + PASSWORD + "\","
                + "      path: \"" + escapeWindowsPath(trustStorePath) + "\""
                + "      }" + NL
                + "  }," + NL
                + "  rest: {" + NL
                + "    port: {}," + NL
                + "    ssl.port: {}" + NL
                + "  }," + NL
                + "  failureHandler.handler.type: noop,"  + NL
                + "  failureHandler.dumpThreadsOnFailure: false" + NL
                + "}";

        @Override
        protected int initialNodes() {
            return 2;
        }

        @Override
        protected String getNodeBootstrapConfigTemplate() {
            return sslEnabledBoostrapConfig;
        }

        @Test
        @DisplayName("SSL enabled and setup correctly then cluster starts")
        void clusterStartsWithEnabledSsl(TestInfo testInfo) {
            assertThat(CLUSTER.runningNodes().count(), is(2L));
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
            assertThrowsSqlException(SQLException.class, () -> DriverManager.getConnection(url));
        }

        @Test
        @DisplayName("Client can not connect without client authentication configured")
        void clientCanNotConnectWithoutClientAuth() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(PASSWORD)
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
        void clientCanConnectWithSslAndClientAuth() {
            var sslConfiguration =
                    SslConfiguration.builder()
                            .enabled(true)
                            .trustStorePath(trustStorePath)
                            .trustStorePassword(PASSWORD)
                            .keyStorePath(keyStorePath)
                            .keyStorePassword(PASSWORD)
                            .build();

            try (IgniteClient client = IgniteClient.builder()
                    .addresses("localhost:10800")
                    .ssl(sslConfiguration)
                    .build()
            ) {
                assertThat(client.cluster().nodes(), hasSize(2));
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
                            + "&trustStorePassword=" + PASSWORD
                            + "&clientAuth=require"
                            + "&keyStorePath=" + keyStorePath
                            + "&keyStoreType=PKCS12"
                            + "&keyStorePassword=" + PASSWORD;
            try (Connection conn = DriverManager.getConnection(url)) {
                // No-op.
            }
        }
    }

    @Test
    @DisplayName("Cluster is not initialized when nodes are configured with incompatible ciphers")
    void incompatibleCiphersNodes(TestInfo testInfo, @WorkDirectory Path workDir) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir).build();

        Cluster incompatibleTestCluster = new Cluster(clusterConfiguration);

        try {
            String sslEnabledWithCipher1BoostrapConfig = createBoostrapConfig("TLS_AES_256_GCM_SHA384");
            String sslEnabledWithCipher2BoostrapConfig = createBoostrapConfig("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");

            ServerRegistration successfulRegistration = incompatibleTestCluster.startEmbeddedNode(0, sslEnabledWithCipher1BoostrapConfig);

            InitParameters initParameters = InitParameters.builder()
                    .metaStorageNodes(successfulRegistration.server())
                    .clusterName("cluster")
                    .build();

            TestIgnitionManager.init(successfulRegistration.server(), initParameters);

            // First node will initialize the cluster with single node successfully since the second node can't connect to it.
            assertThat(successfulRegistration.registrationFuture(), willCompleteSuccessfully());

            ServerRegistration failingRegistration = incompatibleTestCluster.startEmbeddedNode(1, sslEnabledWithCipher2BoostrapConfig);
            assertThat(failingRegistration.registrationFuture(), willTimeoutIn(1, TimeUnit.SECONDS));
        } finally {
            incompatibleTestCluster.shutdown();
        }
    }

    @Language("JSON")
    private String createBoostrapConfig(String ciphers) {
        return "ignite {" + NL
                + "  network: {" + NL
                + "    ssl : {"
                + "      enabled: true," + NL
                + "      ciphers: " + ciphers + ',' + NL
                + "      trustStore: {" + NL
                + "        password: \"" + PASSWORD + "\","
                + "        path: \"" + escapeWindowsPath(trustStorePath) + "\""
                + "      }," + NL
                + "      keyStore: {" + NL
                + "        password: \"" + PASSWORD + "\","
                + "        path: \"" + escapeWindowsPath(keyStorePath) + "\""
                + "      }" + NL
                + "    }," + NL
                + "    port: {}," + NL
                + "    nodeFinder:{" + NL
                + "      netClusterNodes: [ {}, \"localhost:3345\", \"localhost:3346\" ]" + NL
                + "    }" + NL
                + "  }," + NL
                + "  clientConnector.port: {}," + NL
                + "  clientConnector.ssl: {" + NL
                + "    enabled: true, "
                + "    ciphers: " + ciphers + "," + NL
                + "    keyStore: {" + NL
                + "      path: \"" + escapeWindowsPath(keyStorePath) + "\"," + NL
                + "      password: \"" + PASSWORD + "\"" + NL
                + "    }" + NL
                + "  }," + NL
                + "  rest: {" + NL
                + "    port: {}," + NL
                + "    ssl.port: {}" + NL
                + "  }" + NL
                + "  failureHandler.handler.type: noop," + NL
                + "  failureHandler.dumpThreadsOnFailure: false" + NL
                + "}";
    }
}
