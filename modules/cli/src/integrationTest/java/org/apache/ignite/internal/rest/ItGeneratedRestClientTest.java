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

import static org.apache.ignite.rest.client.model.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.rest.client.model.DeploymentStatus.UPLOADING;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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
import java.util.List;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitClient;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
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
import org.apache.ignite.rest.client.model.NodeState;
import org.apache.ignite.rest.client.model.Problem;
import org.apache.ignite.rest.client.model.UnitStatus;
import org.apache.ignite.rest.client.model.UnitVersionStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test for autogenerated ignite rest client.
 */
@MicronautTest(rebuildContext = true)
public class ItGeneratedRestClientTest extends ClusterPerClassIntegrationTest {

    /** Start rest server port. */
    private static final int BASE_REST_PORT = 10300;

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

    @BeforeAll
    void setUp(TestInfo testInfo) {
        firstNodeName = CLUSTER.nodeName(0);

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
            String configuration = clusterConfigurationApi.getClusterConfigurationByPath("ignite.gc.batchSize");

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
            String configuration = nodeConfigurationApi.getNodeConfigurationByPath("ignite.clientConnector.connectTimeout");

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
    void updateClusterConfigurationWithInvalidParam() throws JsonProcessingException {
        ApiException thrown = assertThrows(
                ApiException.class,
                () -> clusterConfigurationApi.updateClusterConfiguration("ignite {\n"
                        + "    security.enabled:true, \n"
                        + "    security.authentication.providers:null\n"
                        + "}")
        );

        Problem problem = objectMapper.readValue(thrown.getResponseBody(), Problem.class);
        assertThat(problem.getStatus(), equalTo(400));
        assertThat(problem.getInvalidParams(), hasSize(1));
        assertThat(problem.getInvalidParams().get(0).getName(), is("ignite.security.authentication.providers"));
        assertThat(problem.getInvalidParams().get(0).getReason(), is("At least one provider is required."));
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
        assertThat(nodeMetricApi.listNodeMetricSources(), containsInAnyOrder(CliIntegrationTest.ALL_METRIC_SOURCES));
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
}
