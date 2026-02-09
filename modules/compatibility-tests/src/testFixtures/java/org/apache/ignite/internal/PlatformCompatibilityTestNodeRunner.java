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

import static org.apache.ignite.internal.ConfigTemplates.NL;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.TestInfo;

/**
 * Old node runner for platform compatibility tests.
 */
@SuppressWarnings("CallToSystemGetenv")
public class PlatformCompatibilityTestNodeRunner {
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {" + NL
            + "  network: {" + NL
            + "    port: {}," + NL
            + "    nodeFinder.netClusterNodes: [ {} ]" + NL
            + "  }," + NL
            + "  storage.profiles: {"
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  }," + NL
            + "  clientConnector.port: {}," + NL
            + "  clientConnector.sendServerExceptionStackTraceToClient: true," + NL
            + "  rest.port: {}," + NL
            + "  failureHandler.handler.type: noop," + NL
            + "  failureHandler.dumpThreadsOnFailure: false" + NL
            + "}";

    /**
     * Entry point.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        String version = getEnvOrThrow("IGNITE_OLD_SERVER_VERSION");
        String workDir = getEnvOrThrow("IGNITE_OLD_SERVER_WORK_DIR");

        int port = Integer.parseInt(getEnvOrThrow("IGNITE_OLD_SERVER_PORT"));
        int httpPort = Integer.parseInt(getEnvOrThrow("IGNITE_OLD_SERVER_HTTP_PORT"));
        int clientPort = Integer.parseInt(getEnvOrThrow("IGNITE_OLD_SERVER_CLIENT_PORT"));

        System.out.println(">>> Starting test node with version: " + version + " in work directory: " + workDir);
        System.out.println(">>> Ports: node=" + port + ", http=" + httpPort + ", client=" + clientPort);

        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(new PlatformTestInfo(), Path.of(workDir))
                .defaultNodeBootstrapConfigTemplate(NODE_BOOTSTRAP_CFG_TEMPLATE)
                .basePort(port)
                .baseHttpPort(httpPort)
                .baseClientPort(clientPort)
                .build();

        var cluster = new IgniteCluster(clusterConfiguration);
        cluster.start(version, 1, Collections.emptyList());
        cluster.init(x -> {});

        try (var client = cluster.createClient()) {
            CompatibilityTestCommon.createDefaultTables(client);
        }

        System.out.println(">>> Started test node with version: " + version);
        System.out.println("THIN_CLIENT_PORTS=" + clusterConfiguration.baseClientPort());
        Thread.sleep(60_000);

        cluster.stop();
    }

    private static String getEnvOrThrow(String name) throws Exception {
        String val = System.getenv(name);

        if (val == null || val.isEmpty()) {
            throw new Exception(name + " environment variable is not set or empty.");
        }

        return val;
    }

    private static class PlatformTestInfo implements TestInfo {
        @Override
        public String getDisplayName() {
            return "PlatformCompatibilityTestNodeRunner";
        }

        @Override
        public Set<String> getTags() {
            return Set.of();
        }

        @Override
        public Optional<Class<?>> getTestClass() {
            return Optional.of(PlatformCompatibilityTestNodeRunner.class);
        }

        @Override
        public Optional<Method> getTestMethod() {
            return Optional.empty();
        }
    }
}
