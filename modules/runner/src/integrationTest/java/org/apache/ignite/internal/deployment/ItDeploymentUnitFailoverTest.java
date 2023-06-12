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

package org.apache.ignite.internal.deployment;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link IgniteDeployment} for recovery logic.
 */
public class ItDeploymentUnitFailoverTest extends ClusterPerTestIntegrationTest {
    private DeployFiles files;

    @BeforeEach
    public void generateDummy() {
        files = new DeployFiles(workDir);
    }

    @Override
    protected int initialNodes() {
        return 9;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[] {6, 7, 8};
    }

    @Test
    public void testDeployWithNodeStop() {
        IgniteImpl cmgNode = cluster.node(8);

        List<String> cmgNodes = Arrays.stream(cmgMetastoreNodes())
                .mapToObj(i -> node(i).name())
                .collect(Collectors.toList());

        // Deploy to all CMG nodes, not only to majority
        Unit big = files.deployAndVerify(
                "id1",
                Version.parseVersion("1.0.0"),
                false,
                List.of(files.bigFile()),
                null,
                cmgNodes,
                cluster.node(3)
        );

        stopNode(8);

        big.waitUnitClean(cmgNode);
        big.waitUnitReplica(cluster.node(6));
        big.waitUnitReplica(cluster.node(7));

        cmgNode = startNode(8);
        big.waitUnitReplica(cmgNode);
    }
}
