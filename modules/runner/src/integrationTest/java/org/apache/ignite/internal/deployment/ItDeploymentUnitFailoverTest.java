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

import java.io.IOException;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link IgniteDeployment} for recovery logic.
 */
@Disabled("IGNITE-19662")
public class ItDeploymentUnitFailoverTest extends ClusterPerTestIntegrationTest {
    private DeployFiles files;

    @BeforeEach
    public void generateDummy() throws IOException {
        files = new DeployFiles(workDir);
    }

    @Override
    protected int initialNodes() {
        return 8;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[] {0, 1, 2};
    }

    @Test
    public void testDeployWithNodeStop() {
        IgniteImpl node0 = cluster.node(0);
        Unit mediumUnit = files.deployAndVerifyBig("id1", Version.parseVersion("1.0.0"), cluster.node(3));

        stopNode(0);

        mediumUnit.waitUnitClean(node0);
        mediumUnit.waitUnitReplica(cluster.node(1));
        mediumUnit.waitUnitReplica(cluster.node(2));

        node0 = startNode(0);
        mediumUnit.waitUnitReplica(node0);
    }
}
