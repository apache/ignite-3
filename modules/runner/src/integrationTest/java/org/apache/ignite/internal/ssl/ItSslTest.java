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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.nio.file.Path;
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

    static String password;

    static String trustStorePath;

    static String keyStorePath;

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
        Path workDir;

        Cluster cluster;

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
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class ClusterWithSsl {

        @WorkDirectory
        Path workDir;

        Cluster cluster;

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
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class ClusterWithSslAndClientAuth {

        @WorkDirectory
        Path workDir;

        Cluster cluster;

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
    }
}
