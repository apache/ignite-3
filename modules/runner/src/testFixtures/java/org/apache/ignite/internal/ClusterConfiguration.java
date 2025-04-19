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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.intellij.lang.annotations.Language;
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

    private static final Map<String, String> DEFAULT_NODE_BOOTSTRAP_CFG_MAP;

    private final TestInfo testInfo;

    private final Path workDir;

    private final String defaultNodeBootstrapConfigTemplate;

    private final String clusterName;

    private final int basePort;

    private final int baseClientPort;

    private final int baseHttpPort;

    private final int baseHttpsPort;

    private final NodeNamingStrategy nodeNamingStrategy;

    private final Function<Integer, String> nodeAttributesProvider;

    static {
        DEFAULT_NODE_BOOTSTRAP_CFG_MAP = new LinkedHashMap<>();
        DEFAULT_NODE_BOOTSTRAP_CFG_MAP.put("network", NetworkValueInjector.BASE_CONFIG);
        DEFAULT_NODE_BOOTSTRAP_CFG_MAP.put("clientConnector", ClientConnectorValueInjector.BASE_CONFIG);
        DEFAULT_NODE_BOOTSTRAP_CFG_MAP.put("rest", RestValueInjector.BASE_CONFIG);
        DEFAULT_NODE_BOOTSTRAP_CFG_MAP.put("nodeAttributes", NodeAttributesValueInjector.BASE_CONFIG);
        DEFAULT_NODE_BOOTSTRAP_CFG_MAP.put("failureHandler", FailureHandlerValueInjector.BASE_CONFIG);
    }

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
            Function<Integer, String> nodeAttributesProvider
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
        this.nodeAttributesProvider = nodeAttributesProvider;
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

    public Function<Integer, String> nodeAttributesProvider() {
        return nodeAttributesProvider;
    }

    public static Builder builder(TestInfo testInfo, Path workDir) {
        return new Builder(testInfo, workDir);
    }

    private static List<ConfigOverride> annotations(TestInfo testInfo) {
        Optional<Class<?>> cls = testInfo.getTestClass();
        Optional<Method> method = testInfo.getTestMethod();

        List<ConfigOverride> annotations = new ArrayList<>();

        ConfigOverride clsOverride = cls.map(c -> c.getAnnotation(ConfigOverride.class)).orElse(null);
        ConfigOverride methodOverride = method.map(m -> m.getAnnotation(ConfigOverride.class)).orElse(null);

        ConfigOverrides clsOverrideMultiple = cls.map(c -> c.getAnnotation(ConfigOverrides.class)).orElse(null);
        ConfigOverrides methodOverrideMultiple = method.map(m -> m.getAnnotation(ConfigOverrides.class)).orElse(null);

        if (clsOverride != null) {
            annotations.add(clsOverride);
        }

        if (methodOverride != null) {
            annotations.add(methodOverride);
        }

        if (clsOverrideMultiple != null) {
            annotations.addAll(List.of(clsOverrideMultiple.value()));
        }

        if (methodOverrideMultiple != null) {
            annotations.addAll(List.of(methodOverrideMultiple.value()));
        }

        return annotations;
    }

    static boolean containsOverrides(TestInfo testInfo, int nodeIndex) {
        List<ConfigOverride> annotations = annotations(testInfo);

        return annotations.stream()
                .anyMatch(a -> a.nodeIndex() == -1 || a.nodeIndex() == nodeIndex);
    }


    static Map<String, String> configWithOverrides(@Nullable TestInfo testInfo, int nodeIndex, List<ValueInjector> injectors) {
        List<ConfigOverride> annotations = testInfo == null ? List.of() : annotations(testInfo);

        Map<String, String> cfg = baseConfig(injectors);

        for (ConfigOverride co : annotations) {
            if (co.nodeIndex() == -1 || co.nodeIndex() == nodeIndex) {
                cfg.put(co.name(), co.value());
            }
        }

        return cfg;
    }

    private static Map<String, String> baseConfig(List<ValueInjector> injectors) {
        Map<String, String> cfg = new HashMap<>(DEFAULT_NODE_BOOTSTRAP_CFG_MAP);

        for (ValueInjector injector : injectors) {
            cfg.put(injector.name(), injector.config());
        }

        return cfg;
    }

    static String assembleConfig(Map<String, String> cfgMap) {
        StringBuilder configBuilder = new StringBuilder();

        configBuilder.append("ignite {\n");

        for (Map.Entry<String, String> e : cfgMap.entrySet()) {
            assertNotNull(e.getValue());

            configBuilder
                    .append("  ")
                    .append(e.getKey())
                    .append(": ")
                    .append(e.getValue())
                    .append("\n");
        }

        configBuilder.append("}");

        return configBuilder.toString();
    }

    /**
     * Builder for {@link ClusterConfiguration}.
     */
    public static class Builder {
        private final TestInfo testInfo;

        private final Path workDir;

        private String defaultNodeBootstrapConfigTemplate = assembleConfig(DEFAULT_NODE_BOOTSTRAP_CFG_MAP);

        private String clusterName = "cluster";

        private int basePort = DEFAULT_BASE_PORT;

        private int baseClientPort = DEFAULT_BASE_CLIENT_PORT;

        private int baseHttpPort = DEFAULT_BASE_HTTP_PORT;

        private int baseHttpsPort = DEFAULT_BASE_HTTPS_PORT;

        private NodeNamingStrategy nodeNamingStrategy = new DefaultNodeNamingStrategy();

        private Function<Integer, String> nodeAttributesProvider = n -> "{}";

        public Builder(TestInfo testInfo, Path workDir) {
            this.testInfo = testInfo;
            this.workDir = workDir;
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

        public Builder nodeNamingStrategy(NodeNamingStrategy nodeNamingStrategy) {
            this.nodeNamingStrategy = nodeNamingStrategy;
            return this;
        }

        public Builder nodeAttributesProvider(Function<Integer, String> nodeAttributesProvider) {
            this.nodeAttributesProvider = nodeAttributesProvider;
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
                    nodeAttributesProvider
            );
        }
    }

    private static class DefaultNodeNamingStrategy implements NodeNamingStrategy {
        @Override
        public String nodeName(ClusterConfiguration clusterConfiguration, int nodeIndex) {
            return testNodeName(clusterConfiguration.testInfo(), clusterConfiguration.basePort + nodeIndex);
        }
    }

    interface ValueInjector {
        String name();

        String config();
    }

    static class NetworkValueInjector implements ValueInjector {
        @Language("HOCON")
        static final String BASE_CONFIG = "{\n"
                + "    port: {},\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ {} ]\n"
                + "    }\n"
                + "  }\n";

        final int port;
        final String netClusterNodes;

        public NetworkValueInjector(int port, String netClusterNodes) {
            this.port = port;
            this.netClusterNodes = netClusterNodes;
        }

        @Override
        public String name() {
            return "network";
        }

        @Override
        public String config() {
            return format(BASE_CONFIG, port, netClusterNodes);
        }
    }

    static class ClientConnectorValueInjector implements ValueInjector {
        @Language("HOCON")
        static final String BASE_CONFIG = "{ port:{} }\n";

        final int port;

        public ClientConnectorValueInjector(int port) {
            this.port = port;
        }

        @Override
        public String name() {
            return "clientConnector";
        }

        @Override
        public String config() {
            return format(BASE_CONFIG, port);
        }
    }

    static class RestValueInjector implements ValueInjector {
        @Language("HOCON")
        static final String BASE_CONFIG = "{\n"
                + "    port: {},\n"
                + "    ssl.port: {}\n"
                + "  }\n";

        final int port;
        final int sslPort;

        public RestValueInjector(int port, int sslPort) {
            this.port = port;
            this.sslPort = sslPort;
        }

        @Override
        public String name() {
            return "rest";
        }

        @Override
        public String config() {
            return format(BASE_CONFIG, port, sslPort);
        }
    }

    static class NodeAttributesValueInjector implements ValueInjector {
        @Language("HOCON")
        static final String BASE_CONFIG = "{\n"
                + "    nodeAttributes: {} "
                + "  }\n";

        final String nodeAttributes;

        public NodeAttributesValueInjector(String nodeAttributes) {
            this.nodeAttributes = nodeAttributes;
        }

        @Override
        public String name() {
            return "nodeAttributes";
        }

        @Override
        public String config() {
            return format(BASE_CONFIG, nodeAttributes);
        }
    }

    static class FailureHandlerValueInjector implements ValueInjector {
        @Language("HOCON")
        static final String BASE_CONFIG = "{\n"
                + "    dumpThreadsOnFailure: {}\n "
                + "  }\n";

        final boolean dumpThreadsOnFailure;

        public FailureHandlerValueInjector(boolean dumpThreadsOnFailure) {
            this.dumpThreadsOnFailure = dumpThreadsOnFailure;
        }

        @Override
        public String name() {
            return "failureHandler";
        }

        @Override
        public String config() {
            return format(BASE_CONFIG, dumpThreadsOnFailure);
        }
    }
}
