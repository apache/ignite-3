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

import static org.apache.ignite.internal.AssignmentsTestUtils.awaitAssignmentsStabilization;
import static org.apache.ignite.internal.CompatibilityTestCommon.TABLE_NAME_TEST;
import static org.apache.ignite.internal.CompatibilityTestCommon.createDefaultTables;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.client.DeploymentUtils.runJob;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.client.DeploymentUtils;
import org.apache.ignite.internal.compute.SendAllMetastorageCommandTypesJob;
import org.apache.ignite.internal.compute.TruncateRaftLogCommand;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** Compatibility tests for metastorage raft snapshot. */
@ParameterizedClass
@MethodSource("baseVersions")
@MicronautTest(rebuildContext = true)
public class ItMetastorageRaftSnapshotCompatibilityTest extends CompatibilityTestBase {
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
        DeploymentUtils.deployJobs();

        createDefaultTables(baseIgnite);

        runSendAllMetastorageCommandTypesJob();
        runTruncateLogCommand();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26923")
    void testMetastorageRaftSnapshotCompatibility() throws InterruptedException {
        cluster.stop();
        cluster.startEmbedded(2);

        awaitAssignmentsStabilization(node(0), TABLE_NAME_TEST);

        checkMetastorage();

        MetaStorageManager newNodeMetastorage = unwrapIgniteImpl(cluster.node(1)).metaStorageManager();
        MetaStorageManager oldNodeMetastorage = unwrapIgniteImpl(cluster.node(0)).metaStorageManager();

        // Assert that new node got all log entries from old one.
        await().until(oldNodeMetastorage::appliedRevision, is(newNodeMetastorage.appliedRevision()));
        ;
    }

    private void checkMetastorage() {
        // Will fail if metastorage is corrupted.
        sql("SELECT * FROM " + TABLE_NAME_TEST);
    }

    private void runSendAllMetastorageCommandTypesJob() {
        runJob(cluster, SendAllMetastorageCommandTypesJob.class, "");
    }

    private void runTruncateLogCommand() {
        runJob(cluster, TruncateRaftLogCommand.class, MetastorageGroupId.INSTANCE.toString());
    }
}
