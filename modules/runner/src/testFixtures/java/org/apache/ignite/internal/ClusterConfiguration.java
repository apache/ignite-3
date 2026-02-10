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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
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
            + "  clientConnector.sendServerExceptionStackTraceToClient: true,\n"
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

    private final NodeNamingStrategy nodeNamingStrategy;

    private final boolean usePreConfiguredStorageProfiles;

    private ClusterConfiguration(
            TestInfo testInfo,
            Path workDir,
            String defaultNodeBootstrapConfigTemplate,
            String clusterName,
            int basePort,
            int baseClientPort,
            int baseHttpPort,
            int baseHttpsPort,
            NodeNamingStrategy nodeNamingStrategy,
            boolean usePreConfiguredStorageProfiles
    ) {
        this.testInfo = testInfo;
        this.workDir = workDir;
        this.defaultNodeBootstrapConfigTemplate = defaultNodeBootstrapConfigTemplate;
        this.clusterName = clusterName;
        this.basePort = basePort;
        this.baseClientPort = baseClientPort;
        this.baseHttpPort = baseHttpPort;
        this.baseHttpsPort = baseHttpsPort;
        this.nodeNamingStrategy = nodeNamingStrategy;
        this.usePreConfiguredStorageProfiles = usePreConfiguredStorageProfiles;
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

    public NodeNamingStrategy nodeNamingStrategy() {
        return nodeNamingStrategy;
    }

    public boolean usePreConfiguredStorageProfiles() {
        return usePreConfiguredStorageProfiles;
    }

    public static Builder builder(TestInfo testInfo, Path workDir) {
        return new Builder(testInfo, workDir);
    }

    private static List<ConfigOverride> annotations(TestInfo testInfo) {
        List<ConfigOverride> annotations = new ArrayList<>();

        testInfo.getTestClass().ifPresent(c -> {
            // Process parent classes first, in reverse order
            List<Class<?>> hierarchy = new ArrayList<>();
            Class<?> currentClass = c;
            while (currentClass != null) {
                hierarchy.add(currentClass);
                currentClass = currentClass.getSuperclass();
            }

            // Process from parent to child
            for (int i = hierarchy.size() - 1; i >= 0; i--) {
                Class<?> clazz = hierarchy.get(i);

                ConfigOverride[] clsOverrides = clazz.getAnnotationsByType(ConfigOverride.class);
                if (clsOverrides.length > 0) {
                    annotations.addAll(Arrays.asList(clsOverrides));
                }
            }
        });

        testInfo.getTestMethod().ifPresent(method -> {
            ConfigOverride[] methodOverrides = method.getAnnotationsByType(ConfigOverride.class);
            if (methodOverrides.length > 0) {
                annotations.addAll(Arrays.asList(methodOverrides));
            }
        });

        return annotations;
    }

    static boolean containsOverrides(TestInfo testInfo, int nodeIndex) {
        List<ConfigOverride> annotations = annotations(testInfo);

        return annotations.stream()
                .anyMatch(a -> a.nodeIndex() == -1 || a.nodeIndex() == nodeIndex);
    }

    static Map<String, String> configOverrides(@Nullable TestInfo testInfo, int nodeIndex) {
        List<ConfigOverride> annotations = testInfo == null ? List.of() : annotations(testInfo);

        Map<String, String> overrides = new HashMap<>();

        for (ConfigOverride co : annotations) {
            if (co.nodeIndex() == -1 || co.nodeIndex() == nodeIndex) {
                overrides.put(co.name(), co.value());
            }
        }

        return overrides;
    }

    /**
     * Builder for {@link ClusterConfiguration}.
     */
    public static class Builder {
        private final TestInfo testInfo;

        private final Path workDir;

        private String defaultNodeBootstrapConfigTemplate = DEFAULT_NODE_BOOTSTRAP_CFG;

        private String clusterName = "cluster";

        private int basePort = DEFAULT_BASE_PORT;

        private int baseClientPort = DEFAULT_BASE_CLIENT_PORT;

        private int baseHttpPort = DEFAULT_BASE_HTTP_PORT;

        private int baseHttpsPort = DEFAULT_BASE_HTTPS_PORT;

        private NodeNamingStrategy nodeNamingStrategy = new DefaultNodeNamingStrategy();

        private boolean usePreConfiguredStorageProfiles = true;

        public Builder(TestInfo testInfo, Path workDir) {
            this.testInfo = testInfo;
            this.workDir = workDir;
        }

        public Builder defaultNodeBootstrapConfigTemplate(String defaultNodeBootstrapConfigTemplate) {
            this.defaultNodeBootstrapConfigTemplate = defaultNodeBootstrapConfigTemplate;
            return this;
        }

        public Builder usePreConfiguredStorageProfiles(boolean usePreConfiguredStorageProfiles) {
            this.usePreConfiguredStorageProfiles = usePreConfiguredStorageProfiles;
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

        public Builder nodeNamingStrategy(NodeNamingStrategy nodeNamingStrategy) {
            this.nodeNamingStrategy = nodeNamingStrategy;
            return this;
        }

        /**
         * Creates a new {@link ClusterConfiguration}.
         */
        public ClusterConfiguration build() {
            return new ClusterConfiguration(
                    testInfo,
                    workDir,
                    defaultNodeBootstrapConfigTemplate,
                    clusterName,
                    basePort,
                    baseClientPort,
                    baseHttpPort,
                    baseHttpsPort,
                    nodeNamingStrategy,
                    usePreConfiguredStorageProfiles
            );
        }
    }

    private static class DefaultNodeNamingStrategy implements NodeNamingStrategy {
        @Override
        public String nodeName(ClusterConfiguration clusterConfiguration, int nodeIndex) {
            return testNodeName(clusterConfiguration.testInfo(), clusterConfiguration.basePort + nodeIndex);
        }
    }
}
