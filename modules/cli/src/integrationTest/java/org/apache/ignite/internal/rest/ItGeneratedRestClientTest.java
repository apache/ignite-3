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

package org.apache.ignite.internal.rest;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.rest.client.model.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.rest.client.model.DeploymentStatus.UPLOADING;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitClient;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.rest.client.api.ClusterConfigurationApi;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.api.DeploymentApi;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.api.NodeMetricApi;
import org.apache.ignite.rest.client.api.TopologyApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ClusterState;
import org.apache.ignite.rest.client.model.DeployMode;
import org.apache.ignite.rest.client.model.InitCommand;
import org.apache.ignite.rest.client.model.MetricSource;
import org.apache.ignite.rest.client.model.NodeState;
import org.apache.ignite.rest.client.model.Problem;
import org.apache.ignite.rest.client.model.UnitStatus;
import org.apache.ignite.rest.client.model.UnitVersionStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for autogenerated ignite rest client.
 */
@ExtendWith(WorkDirectoryExtension.class)
@MicronautTest(rebuildContext = true)
@TestInstance(Lifecycle.PER_CLASS)
public class ItGeneratedRestClientTest {
    /** Start network port for test nodes. */
    private static final int BASE_PORT = 3344;

    /** Start rest server port. */
    private static final int BASE_REST_PORT = 10300;

    private static final int BASE_CLIENT_PORT = 10800;

    @WorkDirectory
    private static Path WORK_DIR;

    private final List<String> clusterNodeNames = new ArrayList<>();

    private final List<Ignite> clusterNodes = new ArrayList<>();

    private ApiClient apiClient;

    private ClusterConfigurationApi clusterConfigurationApi;

    private NodeConfigurationApi nodeConfigurationApi;

    private ClusterManagementApi clusterManagementApi;

    private NodeManagementApi nodeManagementApi;

    private TopologyApi topologyApi;

    private NodeMetricApi nodeMetricApi;

    private DeploymentApi deploymentApi;

    private ObjectMapper objectMapper;

    private String firstNodeName;

    @Inject
    private ApiClientFactory clientFactory;

    private static String buildConfig(int nodeIdx) {
        return "{\n"
                + "  network: {\n"
                + "    port: " + (BASE_PORT + nodeIdx) + ",\n"
                + "    portRange: 1,\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ] \n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector.port: " + (BASE_CLIENT_PORT + nodeIdx) + "\n"
                + "}";
    }

    @BeforeAll
    void setUp(TestInfo testInfo) {
        List<CompletableFuture<Ignite>> futures = IntStream.range(0, 3)
                .mapToObj(i -> startNodeAsync(testInfo, i))
                .collect(toList());

        String metaStorageNode = testNodeName(testInfo, BASE_PORT);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodeNames(List.of(metaStorageNode))
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(metaStorageNode, initParameters);

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            clusterNodes.add(future.join());
        }

        firstNodeName = clusterNodes.get(0).name();

        apiClient = clientFactory.getClient("http://localhost:" + BASE_REST_PORT);

        clusterConfigurationApi = new ClusterConfigurationApi(apiClient);
        nodeConfigurationApi = new NodeConfigurationApi(apiClient);
        clusterManagementApi = new ClusterManagementApi(apiClient);
        nodeManagementApi = new NodeManagementApi(apiClient);
        topologyApi = new TopologyApi(apiClient);
        nodeMetricApi = new NodeMetricApi(apiClient);
        deploymentApi = new DeploymentApi(apiClient);

        objectMapper = new ObjectMapper();
    }

    @AfterAll
    void tearDown() throws Exception {
        List<AutoCloseable> closeables = clusterNodeNames.stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(toList());

        IgniteUtils.closeAll(closeables);
    }

    @Test
    void getClusterConfiguration() {
        assertDoesNotThrow(() -> {
            String configuration = clusterConfigurationApi.getClusterConfiguration();

            assertNotNull(configuration);
            assertFalse(configuration.isEmpty());
        });
    }

    @Test
    void getClusterConfigurationByPath() {
        assertDoesNotThrow(() -> {
            String configuration = clusterConfigurationApi.getClusterConfigurationByPath("rocksDb.defaultRegion");

            assertNotNull(configuration);
            assertFalse(configuration.isEmpty());
        });
    }

    @Test
    void updateTheSameClusterConfiguration() {
        assertDoesNotThrow(() -> {
            String originalConfiguration = clusterConfigurationApi.getClusterConfiguration();

            clusterConfigurationApi.updateClusterConfiguration(originalConfiguration);
            String updatedConfiguration = clusterConfigurationApi.getClusterConfiguration();

            assertNotNull(updatedConfiguration);
            assertEquals(originalConfiguration, updatedConfiguration);
        });
    }

    @Test
    void getClusterConfigurationByPathBadRequest() throws JsonProcessingException {
        var thrown = assertThrows(
                ApiException.class,
                () -> clusterConfigurationApi.getClusterConfigurationByPath("no.such.path")
        );

        assertThat(thrown.getCode(), equalTo(400));

        Problem problem = objectMapper.readValue(thrown.getResponseBody(), Problem.class);
        assertThat(problem.getStatus(), equalTo(400));
        assertThat(problem.getDetail(), containsString("Configuration value 'no' has not been found"));
    }

    @Test
    void getNodeConfiguration() {
        assertDoesNotThrow(() -> {
            String configuration = nodeConfigurationApi.getNodeConfiguration();

            assertNotNull(configuration);
            assertFalse(configuration.isEmpty());
        });
    }

    @Test
    void getNodeConfigurationByPath() {
        assertDoesNotThrow(() -> {
            String configuration = nodeConfigurationApi.getNodeConfigurationByPath("clientConnector.connectTimeout");

            assertNotNull(configuration);
            assertFalse(configuration.isEmpty());
        });
    }

    @Test
    void getNodeConfigurationByPathBadRequest() throws JsonProcessingException {
        var thrown = assertThrows(
                ApiException.class,
                () -> nodeConfigurationApi.getNodeConfigurationByPath("no.such.path")
        );

        assertThat(thrown.getCode(), equalTo(400));

        Problem problem = objectMapper.readValue(thrown.getResponseBody(), Problem.class);
        assertThat(problem.getStatus(), equalTo(400));
        assertThat(problem.getDetail(), containsString("Configuration value 'no' has not been found"));
    }

    @Test
    void updateTheSameNodeConfiguration() {
        assertDoesNotThrow(() -> {
            String originalConfiguration = nodeConfigurationApi.getNodeConfiguration();

            nodeConfigurationApi.updateNodeConfiguration(originalConfiguration);
            String updatedConfiguration = nodeConfigurationApi.getNodeConfiguration();

            assertNotNull(updatedConfiguration);
            assertEquals(originalConfiguration, updatedConfiguration);
        });
    }

    @Test
    void updateNodeConfigurationWithInvalidParam() throws JsonProcessingException {
        ApiException thrown = assertThrows(
                ApiException.class,
                () -> clusterConfigurationApi.updateClusterConfiguration("rocksDb.defaultRegion.cache=invalid")
        );

        Problem problem = objectMapper.readValue(thrown.getResponseBody(), Problem.class);
        assertThat(problem.getStatus(), equalTo(400));
        assertThat(problem.getInvalidParams(), hasSize(1));
    }

    @Test
    void initCluster() {
        assertDoesNotThrow(() -> {
            // in fact, this is the second init that means nothing but just testing that the second init does not throw and exception
            // the main init is done before the test
            clusterManagementApi.init(
                    new InitCommand()
                            .clusterName("cluster")
                            .metaStorageNodes(List.of(firstNodeName))
                            .cmgNodes(List.of())
            );
        });
    }

    @Test
    void clusterState() {
        assertDoesNotThrow(() -> {
            ClusterState clusterState = clusterManagementApi.clusterState();

            assertThat(clusterState, is(notNullValue()));
            assertThat(clusterState.getClusterTag().getClusterName(), is(equalTo("cluster")));
            assertThat(clusterState.getCmgNodes(), contains(firstNodeName));
        });
    }

    @Test
    void initClusterNoSuchNode() throws JsonProcessingException {
        var thrown = assertThrows(
                ApiException.class,
                () -> clusterManagementApi.init(
                        new InitCommand()
                                .clusterName("cluster")
                                .metaStorageNodes(List.of("no-such-node"))
                                .cmgNodes(List.of()))
        );

        assertThat(thrown.getCode(), equalTo(400));

        Problem problem = objectMapper.readValue(thrown.getResponseBody(), Problem.class);
        assertThat(problem.getStatus(), equalTo(400));
        assertThat(problem.getDetail(), containsString("Node \"no-such-node\" is not present in the physical topology"));
    }

    @Test
    void nodeState() throws ApiException {
        NodeState nodeState = nodeManagementApi.nodeState();

        assertThat(nodeState, is(notNullValue()));
        assertThat(nodeState.getState(), is(notNullValue()));
        assertThat(nodeState.getName(), is(firstNodeName));
    }

    @Test
    void logicalTopology() throws ApiException {
        assertThat(topologyApi.logical(), hasSize(3));
    }

    @Test
    void physicalTopology() throws ApiException {
        assertThat(topologyApi.physical(), hasSize(3));
    }

    @Test
    void nodeVersion() throws ApiException {
        assertThat(nodeManagementApi.nodeVersion(), is(notNullValue()));
    }

    @Test
    void nodeMetricSourcesList() throws ApiException {
        MetricSource[] expectedMetricSources = {
                new MetricSource().name("jvm").enabled(false),
                new MetricSource().name("client.handler").enabled(false),
                new MetricSource().name("sql.client").enabled(false),
                new MetricSource().name("sql.plan.cache").enabled(false)
        };

        assertThat(nodeMetricApi.listNodeMetricSources(), hasItems(expectedMetricSources));
    }

    @Test
    void nodeMetricSetsList() throws ApiException {
        assertThat(nodeMetricApi.listNodeMetricSets(), empty());
    }

    @Test
    void enableInvalidNodeMetric() throws JsonProcessingException {
        var thrown = assertThrows(
                ApiException.class,
                () -> nodeMetricApi.enableNodeMetric("no.such.metric")
        );

        assertThat(thrown.getCode(), equalTo(404));

        Problem problem = objectMapper.readValue(thrown.getResponseBody(), Problem.class);
        assertThat(problem.getStatus(), equalTo(404));
        assertThat(problem.getDetail(), containsString("Metrics source with given name doesn't exist: no.such.metric"));
    }

    @Test
    void deployUndeployUnitSync() throws ApiException {
        assertThat(deploymentApi.listClusterStatuses(null), empty());

        // TODO https://issues.apache.org/jira/browse/IGNITE-19295
        String unitId = "test.unit.id";
        String unitVersion = "1.0.0";

        new DeployUnitClient(apiClient).deployUnit(unitId, List.of(emptyFile()), unitVersion, DeployMode.MAJORITY, List.of());

        UnitStatus expectedStatus = new UnitStatus().id(unitId)
                .addVersionToStatusItem((new UnitVersionStatus()).version(unitVersion).status(DEPLOYED));

        await().untilAsserted(() -> assertThat(deploymentApi.listClusterStatuses(null), contains(expectedStatus)));

        assertThat(deploymentApi.listClusterStatusesByUnit(unitId, "0.0.0", null), empty());
        assertThat(deploymentApi.listClusterStatusesByUnit(unitId, unitVersion, null), contains(expectedStatus));

        assertThat(deploymentApi.listClusterStatusesByUnit(unitId, null, List.of(UPLOADING)), empty());
        assertThat(deploymentApi.listClusterStatusesByUnit(unitId, null, List.of(DEPLOYED)), contains(expectedStatus));

        assertThat(deploymentApi.listClusterStatuses(List.of(UPLOADING)), empty());
        assertThat(deploymentApi.listClusterStatuses(List.of(DEPLOYED)), contains(expectedStatus));

        deploymentApi.undeployUnit(unitId, unitVersion);
        await().untilAsserted(() -> assertThat(deploymentApi.listClusterStatuses(null), empty()));
    }

    @Test
    void undeployFailed() throws JsonProcessingException {
        ApiException thrown = assertThrows(
                ApiException.class,
                () -> deploymentApi.undeployUnit("test.unit.id", "0.0.0")
        );

        assertThat(thrown.getCode(), equalTo(404));

        Problem problem = objectMapper.readValue(thrown.getResponseBody(), Problem.class);
        assertThat(problem.getStatus(), equalTo(404));
        assertThat(problem.getDetail(), containsString("Unit test.unit.id:0.0.0 not found"));
    }

    private static File emptyFile() {
        try {
            return Files.createTempFile(WORK_DIR, "empty", "file").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<Ignite> startNodeAsync(TestInfo testInfo, int index) {
        String nodeName = testNodeName(testInfo, BASE_PORT + index);

        clusterNodeNames.add(nodeName);

        return TestIgnitionManager.start(nodeName, buildConfig(index), WORK_DIR.resolve(nodeName));
    }
}
