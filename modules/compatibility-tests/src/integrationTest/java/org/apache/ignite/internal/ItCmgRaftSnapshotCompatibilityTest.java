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

import static org.apache.ignite.internal.jobs.DeploymentUtils.runJob;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.compute.TruncateRaftLogCommand;
import org.apache.ignite.internal.jobs.DeploymentUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** Compatibility tests for CMG raft snapshot. */
@ParameterizedClass
@MethodSource("baseVersions")
public class ItCmgRaftSnapshotCompatibilityTest extends CompatibilityTestBase {
    @Override
    protected boolean restartWithCurrentEmbeddedVersion() {
        return false;
    }

    @Override
    protected int nodesCount() {
        return 2;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        DeploymentUtils.deployJobs();
    }

    @Test
    void testCmgRaftSnapshotCompatibility() {
        int nodeIndex = nodesCount() - 1;

        // We want to include the NodesLeave command in the log.
        cluster.stopRunnerNode(nodeIndex);

        try (IgniteClient ignite = cluster.createClient()) {
            await().until(() -> ignite.cluster().nodes().size(), is(nodesCount() - 1));
        }

        runTruncateLogCommand();

        cluster.stop();

        assertDoesNotThrow(() -> cluster.startEmbedded(nodesCount()));
    }

    private void runTruncateLogCommand() {
        runJob(cluster, TruncateRaftLogCommand.class, CmgGroupId.INSTANCE.toString());
    }
}
