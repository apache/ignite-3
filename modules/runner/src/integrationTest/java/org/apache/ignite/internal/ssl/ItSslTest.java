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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.SslConfiguration;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

/** SSL support integration test. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItSslTest {

    private static String password;

    private static String trustStorePath;

    private static String keyStorePath;

    @BeforeAll
    static void beforeAll() {
        password = "changeit";
        trustStorePath = ItSslTest.class.getClassLoader().getResource("ssl/truststore.jks").getPath();
        keyStorePath = ItSslTest.class.getClassLoader().getResource("ssl/keystore.p12").getPath();
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class ClusterWithoutSsl {

        @WorkDirectory
        private Path workDir;

        private Cluster cluster;

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
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class ClusterWithSsl {

        @WorkDirectory
        private Path workDir;

        private Cluster cluster;

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
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class ClusterWithSslAndClientAuth {

        @WorkDirectory
        private Path workDir;

        private Cluster cluster;

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
                + "      },\n"
                + "  }\n"
                + "}";

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
    }
}
