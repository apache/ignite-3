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

import java.nio.file.Path;
import java.util.Objects;
import org.junit.jupiter.api.TestInfo;

/**
 * Class for configuring {@link Cluster} instances.
 */
public class ClusterConfiguration {
    public static final int DEFAULT_BASE_PORT = 3344;

    public static final int DEFAULT_BASE_CLIENT_PORT = 10800;

    public static final int DEFAULT_BASE_HTTP_PORT = 10300;

    public static final int DEFAULT_BASE_HTTPS_PORT = 10400;

    /** Default nodes bootstrap configuration pattern. */
    private static final String DEFAULT_NODE_BOOTSTRAP_CFG = "ignite {\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }\n"
            + "  rest: {\n"
            + "    port: {},\n"
            + "    ssl.port: {}\n"
            + "  }\n"
            + "}";

    private final TestInfo testInfo;

    private final Path workDir;

    private final String defaultNodeBootstrapConfigTemplate;

    private final String clusterName;

    private final int basePort;

    private final int baseClientPort;

    private final int baseHttpPort;

    private final int baseHttpsPort;

    private ClusterConfiguration(
            TestInfo testInfo,
            Path workDir,
            String defaultNodeBootstrapConfigTemplate,
            String clusterName,
            int basePort,
            int baseClientPort,
            int baseHttpPort,
            int baseHttpsPort
    ) {
        this.testInfo = testInfo;
        this.workDir = workDir;
        this.defaultNodeBootstrapConfigTemplate = defaultNodeBootstrapConfigTemplate;
        this.clusterName = clusterName;
        this.basePort = basePort;
        this.baseClientPort = baseClientPort;
        this.baseHttpPort = baseHttpPort;
        this.baseHttpsPort = baseHttpsPort;
    }

    public TestInfo testInfo() {
        return testInfo;
    }

    public Path workDir() {
        return workDir;
    }

    public String defaultNodeBootstrapConfigTemplate() {
        return defaultNodeBootstrapConfigTemplate;
    }

    public String clusterName() {
        return clusterName;
    }

    public int basePort() {
        return basePort;
    }

    public int baseClientPort() {
        return baseClientPort;
    }

    public int baseHttpPort() {
        return baseHttpPort;
    }

    public int baseHttpsPort() {
        return baseHttpsPort;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link ClusterConfiguration}.
     */
    public static class Builder {
        private TestInfo testInfo;

        private Path workDir;

        private String defaultNodeBootstrapConfigTemplate = DEFAULT_NODE_BOOTSTRAP_CFG;

        private String clusterName = "cluster";

        private int basePort = DEFAULT_BASE_PORT;

        private int baseClientPort = DEFAULT_BASE_CLIENT_PORT;

        private int baseHttpPort = DEFAULT_BASE_HTTP_PORT;

        private int baseHttpsPort = DEFAULT_BASE_HTTPS_PORT;

        public Builder testInfo(TestInfo testInfo) {
            this.testInfo = testInfo;
            return this;
        }

        public Builder workDir(Path workDir) {
            this.workDir = workDir;
            return this;
        }

        public Builder defaultNodeBootstrapConfigTemplate(String defaultNodeBootstrapConfigTemplate) {
            this.defaultNodeBootstrapConfigTemplate = defaultNodeBootstrapConfigTemplate;
            return this;
        }

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder basePort(int basePort) {
            this.basePort = basePort;
            return this;
        }

        public Builder baseClientPort(int baseClientPort) {
            this.baseClientPort = baseClientPort;
            return this;
        }

        public Builder baseHttpPort(int baseHttpPort) {
            this.baseHttpPort = baseHttpPort;
            return this;
        }

        public Builder baseHttpsPort(int baseHttpsPort) {
            this.baseHttpsPort = baseHttpsPort;
            return this;
        }

        /**
         * Creates a new {@link ClusterConfiguration}.
         */
        public ClusterConfiguration build() {
            Objects.requireNonNull(testInfo, "testInfo has not been set");
            Objects.requireNonNull(workDir, "workDir has not been set");

            return new ClusterConfiguration(
                    testInfo,
                    workDir,
                    defaultNodeBootstrapConfigTemplate,
                    clusterName,
                    basePort,
                    baseClientPort,
                    baseHttpPort,
                    baseHttpsPort
            );
        }
    }
}
