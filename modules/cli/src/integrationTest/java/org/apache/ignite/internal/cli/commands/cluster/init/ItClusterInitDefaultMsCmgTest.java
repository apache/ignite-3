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

package org.apache.ignite.internal.cli.commands.cluster.init;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClusterInitCommand}.
 *
 * <p>Because the {@link org.apache.ignite.internal.cli.CliIntegrationTest} extends
 * {@link org.apache.ignite.internal.ClusterPerClassIntegrationTest}, each CLI test case for init has to be placed in a separate
 * test class. It'd ideal to refactor the base classes to have a CLI init test class with multiple test cases.
 * This may be needed if more tests are added.
 */
public class ItClusterInitDefaultMsCmgTest extends CliCommandTestNotInitializedIntegrationBase {
    @Override
    protected int initialNodes() {
        // Use 6 nodes so that there is one that's not in MS and CMG by default.
        return 6;
    }

    @Test
    @DisplayName("Init cluster with default MS and CMG")
    void initClusterWithDefaultMsAndCmg() throws InterruptedException {

        // when
        connect(NODE_URL);

        execute(
                "cluster", "init",
                "--name", "cluster"
        );

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Cluster was initialized successfully")
        );

        // then
        awaitClusterInitialized();

        execute("cluster", "topology", "logical");
        assertExitCodeIsZero();
        for (int i = 0; i < initialNodes(); i++) {
            assertOutputContains(CLUSTER.nodeName(i));
        }

        execute("cluster", "status");
        assertExitCodeIsZero();

        String output = getOutput();

        // Extract node lists using regex
        Pattern pattern = Pattern.compile("cmgNodes: \\[(.*?)], msNodes: \\[(.*?)]");
        Matcher matcher = pattern.matcher(output);
        assertThat("Expected cmgNodes and msNodes lists in output", matcher.find(), is(true));

        // Parse the extracted node lists
        Pattern listPattern = Pattern.compile(", ");
        List<String> cmgNodes = List.of(listPattern.split(matcher.group(1)));
        List<String> msNodes = List.of(listPattern.split(matcher.group(2)));

        // Compare lists with expected
        List<String> expectedNodes = IntStream.range(0, initialNodes() - 1)
                .mapToObj(i -> CLUSTER.nodeName(i))
                .collect(Collectors.toList());
        assertThat("cmgNodes do not match expected nodes", cmgNodes, containsInAnyOrder(expectedNodes.toArray()));
        assertThat("msNodes do not match expected nodes", msNodes, containsInAnyOrder(expectedNodes.toArray()));
    }

    private void awaitClusterInitialized() throws InterruptedException {
        assertTrue(waitForCondition(() -> CLUSTER.runningNodes().count() == initialNodes(), 30_000));
    }
}
