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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.deleteIfExists;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * CMG Raft log compatibility tests.
 */
@ParameterizedClass
@MethodSource("baseVersions")
public class ItCmgRaftLogCompatibilityTest extends CompatibilityTestBase {
    @Override
    protected boolean restartWithCurrentEmbeddedVersion() {
        return false;
    }

    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        Path nodeWorkDir = cluster.runnerNodeWorkDir(0);

        cluster.stop();

        deleteCmgDbDir(nodeWorkDir);
    }

    @AfterEach
    void tearDown()  {
        cluster.stop();
    }

    @Test
    void testCmgRaftLogReapplication() {
        cluster.startEmbedded(nodesCount());

        verifyClusterState(0);
    }

    @Test
    void testCmgRaftLogReplication() {
        final int finalNodeCount = nodesCount() + 1;
        final int lastNodeIndex = finalNodeCount - 1;

        cluster.startEmbedded(finalNodeCount);

        verifyClusterState(lastNodeIndex);
    }

    private void deleteCmgDbDir(Path nodeWorkDir) {
        Path cmgDbDir = new ComponentWorkingDir(nodeWorkDir.resolve("cmg")).dbPath();

        assertTrue(Files.exists(cmgDbDir));
        assertTrue(deleteIfExists(cmgDbDir));
    }

    private void verifyClusterState(int nodeIndex) {
        assertThat(unwrapIgniteImpl(cluster.node(nodeIndex)).clusterManagementGroupManager().clusterState()
                .thenAccept(Assertions::assertNotNull), willCompleteSuccessfully());
    }
}
