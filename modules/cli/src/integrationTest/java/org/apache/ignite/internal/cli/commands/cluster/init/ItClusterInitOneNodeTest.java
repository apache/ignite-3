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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.junit.jupiter.api.Test;

/**
 * Tests that node is immediately available after the cluster init command.
 *
 * <p>Because the {@link org.apache.ignite.internal.cli.CliIntegrationTest} extends
 * {@link org.apache.ignite.internal.ClusterPerClassIntegrationTest}, each CLI test case for init has to be placed in a separate
 * test class. It'd ideal to refactor the base classes to have a CLI init test class with multiple test cases.
 * This may be needed if more tests are added.
 */
public class ItClusterInitOneNodeTest extends CliCommandTestNotInitializedIntegrationBase {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void initCluster() {
        // when
        connect(NODE_URL);

        execute("cluster", "init", "--name", "cluster");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Cluster was initialized successfully")
        );

        // then status is immediately available and initialized
        execute("cluster", "status");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs(format(
                        "[name: cluster, nodes: 1, status: active, cmgNodes: [{}], msNodes: [{}]]" + System.lineSeparator(),
                        CLUSTER.nodeName(0),
                        CLUSTER.nodeName(0)
                ))
        );
    }
}
