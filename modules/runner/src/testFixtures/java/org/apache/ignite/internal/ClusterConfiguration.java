/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;

import java.nio.file.Path;
import java.util.function.Function;
import org.intellij.lang.annotations.Language;
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
    @Language("HOCON")
    private static final String DEFAULT_NODE_BOOTSTRAP_CFG = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }\n"
            + "  rest: {\n"
            + "    port: {},\n"
            + "    ssl.port: {}\n"
            + "  }\n"
            + "  nodeAttributes.nodeAttributes: {}\n"
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

    private final Function<Integer, String> nodeAttributesProvider;

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
}
