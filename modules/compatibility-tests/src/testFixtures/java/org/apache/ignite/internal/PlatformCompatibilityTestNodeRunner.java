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

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.TestInfo;

/**
 * Old node runner for platform compatibility tests.
 */
@SuppressWarnings("CallToSystemGetenv")
public class PlatformCompatibilityTestNodeRunner {
    private static final int CLIENT_PORT = 10952;

    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  clientConnector.sendServerExceptionStackTraceToClient: true,\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    public static void main(String[] args) throws Exception {
        String version = System.getenv("IGNITE_OLD_SERVER_VERSION");
        String workDir = System.getenv("IGNITE_OLD_SERVER_WORK_DIR");

        if (version == null || workDir == null) {
            throw new Exception("IGNITE_OLD_SERVER_VERSION and IGNITE_OLD_SERVER_WORK_DIR environment variables are not set.");
        }

        System.out.println(">>> Starting test node with version: " + version + " in work directory: " + workDir);

        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(new PlatformTestInfo(), Path.of(workDir))
                .defaultNodeBootstrapConfigTemplate(NODE_BOOTSTRAP_CFG_TEMPLATE)
                .basePort(4455)
                .baseHttpPort(11300)
                .baseHttpsPort(11400)
                .baseClientPort(CLIENT_PORT)
                .build();

        var cluster = new IgniteCluster(clusterConfiguration);
        cluster.start(version, 1);
        cluster.init(x -> {});

        System.out.println(">>> Started test node with version: " + version);
        System.out.println("THIN_CLIENT_PORTS=" + CLIENT_PORT);
        Thread.sleep(60_000);

        cluster.stop();
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
