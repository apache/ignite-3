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

package org.apache.ignite.internal.cluster.management;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for initializing a cluster.
 */
public class ItClusterInitTest extends IgniteAbstractTest {
    private final Map<String, CompletableFuture<Ignite>> nodesByName = new HashMap<>();

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(nodesByName.keySet().stream().map(name -> () -> IgnitionManager.stop(name)));
    }

    /**
     * Tests a scenario when a cluster is initialized twice.
     */
    @Test
    void testDoubleInit(TestInfo testInfo) {
        createCluster(testInfo, 2);

        String nodeName = nodesByName.keySet().iterator().next();

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(nodeName)
                .metaStorageNodeNames(List.of(nodeName))
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(initParameters);

        assertThat(allOf(nodesByName.values().toArray(CompletableFuture[]::new)), willCompleteSuccessfully());

        // init is idempotent
        TestIgnitionManager.init(initParameters);

        InitParameters initParametersWithWrongNodesList1 = InitParameters.builder()
                .destinationNodeName(nodeName)
                .metaStorageNodeNames(nodesByName.keySet())
                .clusterName("cluster")
                .build();

        // init should fail if the list of nodes is different
        Exception e = assertThrows(InitException.class, () -> IgnitionManager.init(initParametersWithWrongNodesList1));

        assertThat(e.getMessage(), containsString("Init CMG request denied, reason: CMG node names do not match."));

        InitParameters initParametersWithWrongNodesList2 = InitParameters.builder()
                .destinationNodeName(nodeName)
                .metaStorageNodeNames(List.of(nodeName))
                .clusterName("new name")
                .build();

        // init should fail if cluster names are different
        e = assertThrows(InitException.class, () -> IgnitionManager.init(initParametersWithWrongNodesList2));

        assertThat(e.getMessage(), containsString("Init CMG request denied, reason: Cluster names do not match."));
    }

    private void createCluster(TestInfo testInfo, int numNodes) {
        int[] ports = IntStream.range(3344, 3344 + numNodes).toArray();

        String nodeFinderConfig = Arrays.stream(ports)
                .mapToObj(port -> String.format("\"localhost:%d\"", port))
                .collect(Collectors.joining(", ", "[", "]"));

        for (int port : ports) {
            String config = "{"
                    + " network.port: " + port + ","
                    + " clientConnector.port: " + (port + 8000) + ","
                    + " network.nodeFinder.netClusterNodes: " + nodeFinderConfig + ","
                    + " rest.port: " + (port + 10000)
                    + "}";

            String nodeName = testNodeName(testInfo, port);

            nodesByName.put(nodeName, TestIgnitionManager.start(nodeName, config, workDir.resolve(nodeName)));
        }
    }
}
