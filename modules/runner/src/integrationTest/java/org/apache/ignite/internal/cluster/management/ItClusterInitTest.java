/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for initializing a cluster.
 */
public class ItClusterInitTest extends IgniteAbstractTest {
    private final List<String> nodeNames = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        List<AutoCloseable> closeables = nodeNames.stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(Collectors.toList());

        IgniteUtils.closeAll(closeables);
    }

    /**
     * Tests a scenario when a cluster is initialized twice.
     */
    @Test
    void testDoubleInit(TestInfo testInfo) throws Exception {
        createCluster(testInfo, 2);

        String nodeName = nodeNames.get(0);

        IgnitionManager.init(nodeName, List.of(nodeName), "cluster");

        // init is idempotent
        IgnitionManager.init(nodeName, List.of(nodeName), "cluster");

        // TODO: remove 'waitForCondition' after https://issues.apache.org/jira/browse/IGNITE-16811 is fixed
        assertTrue(
                waitForCondition(() -> {
                    // init should fail if the list of nodes is different
                    Exception e = assertThrows(InitException.class, () -> IgnitionManager.init(nodeName, nodeNames, "cluster"));

                    return e.getMessage().contains("Init CMG request denied, reason: CMG node names do not match.");
                }, 10_000)
        );

        // init should fail if cluster names are different
        Exception e = assertThrows(InitException.class, () -> IgnitionManager.init(nodeName, List.of(nodeName), "new name"));

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
                    + " network.nodeFinder.netClusterNodes: " + nodeFinderConfig
                    + "}";

            String nodeName = testNodeName(testInfo, port);

            IgnitionManager.start(nodeName, config, workDir.resolve(nodeName));

            nodeNames.add(nodeName);
        }
    }
}
