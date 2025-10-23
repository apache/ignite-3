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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCode;
import static org.apache.ignite.internal.util.CollectionUtils.setListAtIndex;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.Cluster.ServerRegistration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.api.cluster.InitCommand;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.gradle.tooling.GradleConnectionException;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.gradle.tooling.model.build.BuildEnvironment;

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
    private List<RunnerNode> runnerNodes;

    private volatile boolean started = false;
    private volatile boolean stopped = false;

    private final ClusterConfiguration clusterConfiguration;

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

        runnerNodes = startRunnerNodes(igniteVersion, nodesCount, extraIgniteModuleIds);
    }

    /**
     * Starts cluster in embedded mode with nodes of current version.
     *
     * @param nodesCount Number of nodes in the cluster.
     */
    public void startEmbedded(int nodesCount, boolean initCluster) {
        if (started) {
            throw new IllegalStateException("The cluster is already started");
        }

        // Reset the flag before starting nodes.
        stopped = false;

        List<ServerRegistration> nodeRegistrations = new ArrayList<>();
        for (int nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++) {
            nodeRegistrations.add(startEmbeddedNode(nodeIndex));
        }

        if (initCluster) {
            init(x -> {});
        }

        for (ServerRegistration registration : nodeRegistrations) {
            assertThat(registration.registrationFuture(), willCompleteSuccessfully());
        }

        started = true;
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

        if (runnerNodes != null) {
            List<String> nodeNames = runnerNodes.stream()
                    .map(RunnerNode::nodeName)
                    .collect(toList());

            LOG.info("Shutting the runner nodes down: [nodes={}]", nodeNames);

            runnerNodes.parallelStream().forEach(RunnerNode::stop);
            runnerNodes.clear();

            LOG.info("Shutting down nodes is complete: [nodes={}]", nodeNames);
        }

        started = false;
        stopped = true;
    }

    /**
     * Initializes the cluster using REST API on the first node with default settings.
     */
    void init(Consumer<InitParametersBuilder> initParametersConfigurator) {
        init(new int[] { 0 }, initParametersConfigurator);
    }

    /**
     * Initializes the cluster using REST API on the first node with specified Metastorage and CMG nodes.
     *
     * @param cmgNodes Indices of the CMG nodes (also used as Metastorage group).
     */
    void init(int[] cmgNodes, Consumer<InitParametersBuilder> initParametersConfigurator) {
        // Wait for the node to start accepting requests
        await()
                .ignoreExceptions()
                .timeout(30, TimeUnit.SECONDS)
                .until(
                        () -> send(get("/management/v1/node/state")).body(),
                        hasJsonPath("$.state", is(equalTo("STARTING")))
                );

        // Initialize the cluster
        List<String> metaStorageAndCmgNodes = Arrays.stream(cmgNodes)
                .mapToObj(this::nodeName)
                .collect(toList());

        InitParametersBuilder builder = InitParameters.builder()
                .metaStorageNodeNames(metaStorageAndCmgNodes)
                .clusterName(clusterConfiguration.clusterName());

        initParametersConfigurator.accept(builder);

        sendInitRequest(builder.build());

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
        return IgniteClient.builder().addresses("localhost:" + clientPort()).build();
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

    /** Returns base client port number from cluster configuration. */
    public int clientPort() {
        return clusterConfiguration.baseClientPort();
    }

    private ServerRegistration startEmbeddedNode(int nodeIndex) {
        String nodeName = nodeName(nodeIndex);

        IgniteServer node = TestIgnitionManager.start(
                nodeName,
                null,
                clusterConfiguration.workDir().resolve(clusterConfiguration.clusterName()).resolve(nodeName)
        );

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

    private List<RunnerNode> startRunnerNodes(String igniteVersion, int nodesCount, List<String> extraIgniteModuleIds) {
        try (ProjectConnection connection = GradleConnector.newConnector()
                .forProjectDirectory(getProjectRoot())
                .connect()
        ) {
            BuildEnvironment environment = connection.model(BuildEnvironment.class).get();

            File javaHome = environment.getJava().getJavaHome();

            Set<String> dependencyIds = new HashSet<>();
            dependencyIds.add(IGNITE_RUNNER_DEPENDENCY_ID);
            dependencyIds.addAll(extraIgniteModuleIds);

            String dependenciesListNotation = dependencyIds.stream()
                    .map(dependency -> dependency + ":" + igniteVersion)
                    .collect(Collectors.joining(","));

            File argFile = constructArgFile(connection, dependenciesListNotation, false);

            List<RunnerNode> result = new ArrayList<>();
            for (int nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++) {
                result.add(RunnerNode.startNode(javaHome, argFile, igniteVersion, clusterConfiguration, nodesCount, nodeIndex));
            }

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File getProjectRoot() {
        var absPath = new File("").getAbsolutePath();

        // Find root by looking for "gradlew" file.
        while (!new File(absPath, "gradlew").exists()) {
            var parent = new File(absPath).getParentFile();
            if (parent == null) {
                throw new IllegalStateException("Could not find project root with 'gradlew' file");
            }
            absPath = parent.getAbsolutePath();
        }

        return new File(absPath);
    }

    static File constructArgFile(
            ProjectConnection connection,
            String dependencyNotation,
            boolean classPathOnly) throws IOException {
        File argFilePath = File.createTempFile("argFilePath", "");
        argFilePath.deleteOnExit();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            connection.newBuild()
                    .forTasks(":ignite-compatibility-tests:constructArgFile")
                    .withArguments(
                            "-PdependencyNotation=" + dependencyNotation,
                            "-PargFilePath=" + argFilePath,
                            "-PclassPathOnly=" + classPathOnly
                    )
                    .setStandardOutput(baos)
                    .setStandardError(baos)
                    .run();
        } catch (GradleConnectionException | IllegalStateException e) {
            LOG.error("Failed to run constructArgFile task", e);
            LOG.error("Gradle task output:" + System.lineSeparator() + baos);
            throw new RuntimeException(e);
        }

        return argFilePath;
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

    private Builder newBuilder(String path) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + clusterConfiguration.baseHttpPort() + path));
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return client.send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
