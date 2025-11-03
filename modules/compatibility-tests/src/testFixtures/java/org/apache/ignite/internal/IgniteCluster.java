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

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.ClusterConfiguration.configOverrides;
import static org.apache.ignite.internal.ClusterConfiguration.containsOverrides;
import static org.apache.ignite.internal.Dependencies.constructArgFile;
import static org.apache.ignite.internal.Dependencies.getProjectRoot;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCode;
import static org.apache.ignite.internal.util.CollectionUtils.setListAtIndex;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.client.BasicAuthenticator;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientAuthenticator;
import org.apache.ignite.internal.Cluster.ServerRegistration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.api.cluster.InitCommand;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.gradle.tooling.model.build.BuildEnvironment;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Cluster of nodes. Can be started with nodes of previous Ignite versions running in the external processes or in the embedded mode
 * using current sources.
 */
public class IgniteCluster {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteCluster.class);

    private static final String IGNITE_RUNNER_DEPENDENCY_ID = "org.apache.ignite:ignite-runner";

    // Embedded nodes
    private final List<IgniteServer> igniteServers = new CopyOnWriteArrayList<>();
    private final List<Ignite> nodes = new CopyOnWriteArrayList<>();
    private final HttpClient client = HttpClient.newBuilder().build();

    // External process nodes
    private final List<RunnerNode> runnerNodes = new CopyOnWriteArrayList<>();

    private volatile boolean started = false;
    private volatile boolean stopped = false;

    private final ClusterConfiguration clusterConfiguration;
    private @Nullable IgniteClientAuthenticator authenticator;

    IgniteCluster(ClusterConfiguration clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
    }

    /**
     * Starts cluster with nodes of previous version using external process.
     *
     * @param igniteVersion Ignite version to run the nodes with.
     * @param nodesCount Number of nodes in the cluster.
     * @param extraIgniteModuleIds Gradle dependency id notation of the extra dependencies
     *                             that should be loaded together with requested ignite version.
     */
    public void start(String igniteVersion, int nodesCount, List<String> extraIgniteModuleIds) {
        if (started) {
            throw new IllegalStateException("The cluster is already started");
        }

        startRunnerNodes(igniteVersion, nodesCount, extraIgniteModuleIds);
    }

    /**
     * Starts cluster in embedded mode with nodes of current version.
     *
     * @param nodesCount Number of nodes in the cluster.
     */
    public void startEmbedded(int nodesCount) {
        startEmbedded(null, nodesCount);
    }

    /**
     * Starts cluster in embedded mode with nodes of current version.
     *
     * @param testInfo Test info.
     * @param nodesCount Number of nodes in the cluster.
     */
    public void startEmbedded(
            @Nullable TestInfo testInfo,
            int nodesCount
    ) {
        List<ServerRegistration> nodeRegistrations = startEmbeddedNotInitialized(testInfo, nodesCount);

        for (ServerRegistration registration : nodeRegistrations) {
            assertThat(registration.registrationFuture(), willCompleteSuccessfully());
        }

        started = true;
    }

    /**
     * Starts cluster in embedded mode with nodes of current version.
     *
     * @param nodesCount Number of nodes in the cluster.
     *
     * @return a list of server registrations, one for each node.
     */
    public List<ServerRegistration> startEmbeddedNotInitialized(int nodesCount) {
        return startEmbeddedNotInitialized(null, nodesCount);
    }

    /**
     * Starts cluster in embedded mode with nodes of current version.
     *
     * @param testInfo Test info.
     * @param nodesCount Number of nodes in the cluster.
     *
     * @return a list of server registrations, one for each node.
     */
    public List<ServerRegistration> startEmbeddedNotInitialized(
            @Nullable TestInfo testInfo,
            int nodesCount
    ) {
        if (started) {
            throw new IllegalStateException("The cluster is already started");
        }

        // Reset the flag before starting nodes.
        stopped = false;

        List<ServerRegistration> nodeRegistrations = new ArrayList<>();
        for (int nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++) {
            nodeRegistrations.add(startEmbeddedNode(testInfo, nodeIndex, nodesCount));
        }

        started = true;

        return nodeRegistrations;
    }

    /**
     * Stops all the nodes in the cluster.
     */
    public void stop() {
        List<IgniteServer> serversToStop = new ArrayList<>(igniteServers);

        List<String> serverNames = serversToStop.stream()
                .filter(Objects::nonNull)
                .map(IgniteServer::name)
                .collect(toList());

        LOG.info("Shutting the embedded cluster down [nodes={}]", serverNames);

        Collections.fill(igniteServers, null);
        Collections.fill(nodes, null);

        serversToStop.parallelStream().filter(Objects::nonNull).forEach(IgniteServer::shutdown);

        LOG.info("Shut the embedded cluster down");

        List<String> nodeNames = runnerNodes.stream()
                .map(RunnerNode::nodeName)
                .collect(toList());

        LOG.info("Shutting the runner nodes down: [nodes={}]", nodeNames);

        runnerNodes.parallelStream().forEach(RunnerNode::stop);
        runnerNodes.clear();

        LOG.info("Shutting down nodes is complete: [nodes={}]", nodeNames);

        started = false;
        stopped = true;
    }

    /**
     * Init a cluster running in embedded mode. Only required if this has not been done before in a prior run.
     *
     * @param nodeRegistrations list of server registrations.
     * @param initParametersConfigurator the consumer to use for configuration.
     */
    public void initEmbedded(List<ServerRegistration> nodeRegistrations, Consumer<InitParametersBuilder> initParametersConfigurator) {
        init(initParametersConfigurator);

        for (ServerRegistration registration : nodeRegistrations) {
            assertThat(registration.registrationFuture(), willCompleteSuccessfully());
        }
    }

    /**
     * Initializes the cluster using REST API on the first node with default settings.
     */
    public void init(Consumer<InitParametersBuilder> initParametersConfigurator) {
        int[] cmgNodes = { 0 };
        InitParameters initParameters = initParameters(cmgNodes, initParametersConfigurator);

        authenticator = authenticator(initParameters);

        init(initParameters);
    }

    /**
     * Initializes the cluster using REST API on the first node with specified Metastorage and CMG nodes.
     */
    private void init(InitParameters initParameters) {
        // Wait for the node to start accepting requests
        await()
                .ignoreExceptions()
                .timeout(30, TimeUnit.SECONDS)
                .until(
                        () -> send(get("/management/v1/node/state")).body(),
                        hasJsonPath("$.state", anyOf(equalTo("STARTING"), equalTo("STARTED")))
                );

        // Initialize the cluster
        sendInitRequest(initParameters);

        // Wait for the cluster to be initialized
        await()
                .ignoreExceptions()
                .timeout(30, TimeUnit.SECONDS)
                .until(
                        () -> send(get("/management/v1/node/state")).body(),
                        hasJsonPath("$.state", is(equalTo("STARTED")))
                );

        started = true;
        stopped = false;
    }

    private InitParameters initParameters(int[] cmgNodes, Consumer<InitParametersBuilder> initParametersConfigurator) {
        List<String> metaStorageAndCmgNodes = Arrays.stream(cmgNodes)
                .mapToObj(this::nodeName)
                .collect(toList());

        InitParametersBuilder builder = InitParameters.builder()
                .metaStorageNodeNames(metaStorageAndCmgNodes)
                .clusterName(clusterConfiguration.clusterName());

        initParametersConfigurator.accept(builder);

        return builder.build();
    }

    private void sendInitRequest(InitParameters initParameters) {
        ObjectMapper mapper = new ObjectMapper();
        String requestBody;
        try {
            InitCommand initCommand = new InitCommand(
                    initParameters.metaStorageNodeNames(),
                    initParameters.cmgNodeNames(),
                    initParameters.clusterName(),
                    initParameters.clusterConfiguration()
            );
            requestBody = mapper.writeValueAsString(initCommand);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        assertThat(send(post("/management/v1/cluster/init", requestBody)), hasStatusCode(200));
    }

    /**
     * Creates a client connection to the first node of the cluster.
     *
     * @return Ignite client instance.
     */
    public IgniteClient createClient() {
        return createClient(authenticator);
    }

    private IgniteClient createClient(@Nullable IgniteClientAuthenticator authenticator) {
        IgniteClient.Builder builder = IgniteClient.builder().addresses("localhost:" + clusterConfiguration.baseClientPort());

        if (authenticator != null) {
            builder.authenticator(authenticator);
        }

        return builder.build();
    }

    /**
     * Returns target version embedded node.
     *
     * @param index Node index.
     * @return Embedded node.
     */
    public Ignite node(int index) {
        return Objects.requireNonNull(nodes.get(index), "index=" + index);
    }

    /**
     * Returns node name by index.
     *
     * @param nodeIndex Index of the node.
     * @return Node name.
     */
    public String nodeName(int nodeIndex) {
        return clusterConfiguration.nodeNamingStrategy().nodeName(clusterConfiguration, nodeIndex);
    }

    /**
     * Returns cluster nodes.
     *
     * @return Cluster nodes.
     */
    public List<Ignite> nodes() {
        return nodes;
    }

    /**
     * Starts an embedded node with the given index.
     *
     * @param testInfo Test info.
     * @param nodeIndex Index of the node to start.
     * @param nodesCount the total number of nodes in the cluster.
     * @return Server registration and future that completes when the node is fully started and joined the cluster.
     */
    public ServerRegistration startEmbeddedNode(
            @Nullable TestInfo testInfo,
            int nodeIndex,
            int nodesCount
    ) {
        String nodeName = nodeName(nodeIndex);
        String config = formatConfig(clusterConfiguration, nodeName, nodeIndex, nodesCount);

        if (testInfo != null && containsOverrides(testInfo, nodeIndex)) {
            config = TestIgnitionManager.applyOverridesToConfig(config, configOverrides(testInfo, nodeIndex));
        }

        IgniteServer node = clusterConfiguration.usePreConfiguredStorageProfiles()
                ? TestIgnitionManager.start(
                nodeName,
                config,
                clusterConfiguration.workDir().resolve(clusterConfiguration.clusterName()).resolve(nodeName))
                : TestIgnitionManager.startWithoutPreConfiguredStorageProfiles(
                        nodeName,
                        config,
                        clusterConfiguration.workDir().resolve(clusterConfiguration.clusterName()).resolve(nodeName));

        synchronized (igniteServers) {
            setListAtIndex(igniteServers, nodeIndex, node);
        }

        CompletableFuture<Void> registrationFuture = node.waitForInitAsync().thenRun(() -> {
            synchronized (nodes) {
                setListAtIndex(nodes, nodeIndex, node.api());
            }

            if (stopped) {
                // Make sure we stop even a node that finished starting after the cluster has been stopped.
                node.shutdown();
            }
        });

        return new ServerRegistration(node, registrationFuture);
    }

    private void startRunnerNodes(String igniteVersion, int nodesCount, List<String> extraIgniteModuleIds) {
        try (ProjectConnection connection = getProjectConnection()) {
            File javaHome = getJavaHome(connection);
            File argFile = getArgsFile(connection, igniteVersion, extraIgniteModuleIds);

            for (int nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++) {
                String nodeName = clusterConfiguration.nodeNamingStrategy().nodeName(clusterConfiguration, nodeIndex);
                String nodeConfig = formatConfig(clusterConfiguration, nodeName, nodeIndex, nodesCount);
                RunnerNode newNode = RunnerNode.startNode(javaHome, argFile, igniteVersion, clusterConfiguration, nodeConfig, nodesCount,
                        nodeName);

                runnerNodes.add(newNode);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private HttpRequest post(String path, String body) {
        return newBuilder(path)
                .header("content-type", "application/json")
                .POST(BodyPublishers.ofString(body))
                .build();
    }

    private HttpRequest get(String path) {
        return newBuilder(path).build();
    }

    private HttpRequest get(String path, int nodeIndex) {
        return newBuilder(path, nodeIndex).build();
    }

    private Builder newBuilder(String path, int nodeIndex) {
        Builder builder = HttpRequest.newBuilder(URI.create("http://localhost:" + port(nodeIndex) + path));

        if (authenticator instanceof BasicAuthenticator) {
            builder.header("Authorization", basicAuthenticationHeader((BasicAuthenticator) authenticator));
        }

        return builder;
    }

    private Builder newBuilder(String path) {
        return newBuilder(path, 0);
    }

    private int port(int nodeIndex) {
        return clusterConfiguration.baseHttpPort() + nodeIndex;
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return client.send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns cluster name. */
    public String clusterName() {
        return clusterConfiguration.clusterName();
    }

    /** Returns list of runner nodes. */
    public List<RunnerNode> getRunnerNodes() {
        return runnerNodes;
    }

    /** Returns embedded node's work directory. */
    public Path embeddedNodeWorkDir(int nodeIndex) {
        return workDir(nodeIndex, true);
    }

    /** Returns runner node's work directory. */
    public Path runnerNodeWorkDir(int nodeIndex) {
        return workDir(nodeIndex, false);
    }

    private Path workDir(int nodeIndex, boolean embedded) {
        String nodeName = embedded ? igniteServers.get(nodeIndex).name() : runnerNodes.get(nodeIndex).nodeName();

        return clusterConfiguration.workDir().resolve(clusterConfiguration.clusterName()).resolve(nodeName);
    }

    private static String seedAddressesString(ClusterConfiguration clusterConfiguration, int seedsCount) {
        return IntStream.range(0, seedsCount)
                .map(nodeIndex -> clusterConfiguration.basePort() + nodeIndex)
                .mapToObj(port -> "\"localhost:" + port + '\"')
                .collect(joining(", "));
    }

    private static String formatConfig(ClusterConfiguration clusterConfiguration, String nodeName, int nodeIndex, int nodesCount) {
        return format(
                clusterConfiguration.defaultNodeBootstrapConfigTemplate(),
                clusterConfiguration.basePort() + nodeIndex,
                seedAddressesString(clusterConfiguration, nodesCount),
                clusterConfiguration.baseClientPort() + nodeIndex,
                clusterConfiguration.baseHttpPort() + nodeIndex,
                clusterConfiguration.baseHttpsPort() + nodeIndex,
                nodeName,
                nodeIndex
        );
    }

    private static ProjectConnection getProjectConnection() {
        return GradleConnector.newConnector()
                .forProjectDirectory(getProjectRoot())
                .connect();
    }

    private static File getJavaHome(ProjectConnection connection) {
        BuildEnvironment environment = connection.model(BuildEnvironment.class).get();

        return environment.getJava().getJavaHome();
    }

    private static File getArgsFile(ProjectConnection connection, String igniteVersion, List<String> extraIgniteModuleIds)
            throws IOException {
        Set<String> dependencyIds = new HashSet<>();
        dependencyIds.add(IGNITE_RUNNER_DEPENDENCY_ID);
        dependencyIds.addAll(extraIgniteModuleIds);

        String dependenciesListNotation = dependencyIds.stream()
                .map(dependency -> dependency + ":" + igniteVersion)
                .collect(joining(","));

        return constructArgFile(connection, dependenciesListNotation, false);
    }

    private static String basicAuthenticationHeader(BasicAuthenticator authenticator) {
        String valueToEncode = authenticator.identity() + ":" + authenticator.secret();
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }

    /**
     * Parses the cluster configuration and returns {@link BasicAuthenticator} if there is a user with "system" role.
     *
     * @see ClusterSecurityConfigurationBuilder
     */
    private static @Nullable IgniteClientAuthenticator authenticator(InitParameters initParameters) {
        if (initParameters.clusterConfiguration() == null) {
            return null;
        }

        Config cfg = ConfigFactory.parseString(initParameters.clusterConfiguration());

        if (!cfg.hasPath("ignite.security.enabled")
                || !cfg.getBoolean("ignite.security.enabled")
                || !cfg.hasPath("ignite.security.authentication.providers")) {
            return null;
        }

        return cfg.getConfigList("ignite.security.authentication.providers")
                .stream()
                .filter(provider -> "basic".equalsIgnoreCase(provider.getString("type")))
                .flatMap(provider -> provider.getConfigList("users").stream())
                .filter(user -> user.getStringList("roles").contains("system"))
                .findAny()
                .map(user -> BasicAuthenticator.builder()
                        .username(user.getString("username"))
                        .password(user.getString("password"))
                        .build()
                )
                .orElseThrow();
    }
}
