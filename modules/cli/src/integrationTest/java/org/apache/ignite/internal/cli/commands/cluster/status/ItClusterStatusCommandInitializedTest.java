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

package org.apache.ignite.internal.cli.commands.cluster.status;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClusterStatusCommand} for the cluster that is initialized.
 */
class ItClusterStatusCommandInitializedTest extends CliIntegrationTest {
    private Function<int[], String> mapper;

    @Override
    protected @Nullable int[] metastoreNodes() {
        return new int[] { 0 };
    }

    @Override
    protected @Nullable int[] cmgNodes() {
        return new int[] { 1 };
    }

    @Test
    @DisplayName("Should print status when valid cluster url is given but cluster is initialized")
    void printStatus() throws InterruptedException {
        String node0Url = NODE_URL;
        String node1Url = "http://localhost:" + CLUSTER.httpPort(1);

        Map<Integer, String> nodeNames = IntStream.range(0, initialNodes())
                .boxed()
                .collect(Collectors.toMap(identity(), i ->  CLUSTER.node(i).name()));

        mapper = nodes -> Arrays.stream(nodes)
                .mapToObj(nodeNames::get)
                .collect(joining(", ", "[", "]"));

//        CLUSTER.stopNode(0);
//        execute("cluster", "status", "--url", node1Url);
//        assertOutput("cluster", 2, "Metastore majority lost", cmgNodes(), metastoreNodes());

//        CLUSTER.startNode(0);
//        execute("cluster", "status", "--url", node1Url);
//        assertOutput("cluster", 3, "active", cmgNodes(), metastoreNodes());

//        CLUSTER.stopNode(1);
//        execute("cluster", "status", "--url", node0Url);
//        assertOutput("N/A", 2, "CMG majority lost", new int[0], new int[0]);
    }

    private void assertOutput(
            String name,
            int nodesCount,
            String statusStr,
            int[] cmgNodes,
            int[] metastoreNodes
    ) {
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("name: " + name),
                () -> assertOutputContains("nodes: " + nodesCount),
                () -> assertOutputContains("status: " + statusStr),
                () -> assertOutputContains("cmgNodes: " + mapper.apply(cmgNodes)),
                () -> assertOutputContains("msNodes: " + mapper.apply(metastoreNodes))
        );
    }
}
