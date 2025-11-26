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

package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.BAR_KEY;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.FOO_KEY;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.VALUE;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.allNodesContainSingleRevisionForKeyLocally;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.createClusterConfigWithCompactionProperties;
import static org.apache.ignite.internal.metastorage.TestMetasStorageUtils.latestKeyRevision;
import static org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTriggerConfiguration.DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME;
import static org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTriggerConfiguration.INTERVAL_SYSTEM_PROPERTY_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngineV2;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.CompactionCommand;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** For {@link MetaStorageCompactionTrigger} testing for single node case. */
@WithSystemProperty(key = DistributionZoneRebalanceEngineV2.SKIP_REBALANCE_TRIGGERS_RECOVERY, value = "true")
public class ItMetaStorageCompactionTriggerOneNodeTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);

        builder.clusterConfiguration(createClusterConfigWithCompactionProperties(10, 10));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-25229")
    void testCompactionAfterRestartNode() throws Exception {
        IgniteImpl node = aliveNode();

        MetaStorageManager metaStorageManager = node.metaStorageManager();

        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());
        assertThat(metaStorageManager.put(BAR_KEY, VALUE), willCompleteSuccessfully());

        // Let's wait until the compaction on revision of FOO_KEY creation happens.
        long fooRevision = latestKeyRevision(metaStorageManager, FOO_KEY);
        assertTrue(waitForCondition(() -> metaStorageManager.getCompactionRevisionLocally() >= fooRevision, 10, 1_000));

        log.info("Latest revision for key: [key={}, revision={}]", FOO_KEY, fooRevision);

        // Let's cancel new compactions to create a new version for the key and not compact it until we restart the node.
        startDroppingCompactionCommand(node);
        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());

        long latestFooRevision = latestKeyRevision(metaStorageManager, FOO_KEY);

        long latestCompactionRevision = metaStorageManager.getCompactionRevisionLocally();
        // Let's change the properties before restarting so that a new scheduled compaction does not start after the node starts.
        changeCompactionProperties(node, Long.MAX_VALUE, Long.MAX_VALUE);

        IgniteImpl restartedNode = restartNode();

        MetaStorageManager restartedMetaStorageManager = restartedNode.metaStorageManager();

        // Let's make sure that after the restart the correct revision of the compaction is restored and the compaction itself will be at
        // the latest compaction revision.
        assertEquals(latestCompactionRevision, restartedMetaStorageManager.getCompactionRevisionLocally());
        assertTrue(waitForCondition(() -> allNodesContainSingleRevisionForKeyLocally(cluster, FOO_KEY, latestFooRevision), 10, 1_000));
    }

    private IgniteImpl aliveNode() {
        return unwrapIgniteImpl(node(0));
    }

    private IgniteImpl restartNode() {
        return unwrapIgniteImpl(restartNode(0));
    }

    private static void startDroppingCompactionCommand(IgniteImpl node) {
        node.dropMessages((s, message) -> message instanceof WriteActionRequest
                && ((WriteActionRequest) message).deserializedCommand() instanceof CompactionCommand);
    }

    private static void changeCompactionProperties(IgniteImpl node, long interval, long dataAvailabilityTime) {
        CompletableFuture<Void> changeFuture = node
                .clusterConfiguration()
                .getConfiguration(SystemDistributedExtensionConfiguration.KEY)
                .system()
                .properties()
                .change(systemPropertyViews -> systemPropertyViews
                        .update(
                                INTERVAL_SYSTEM_PROPERTY_NAME,
                                systemPropertyChange -> systemPropertyChange.changePropertyValue(Long.toString(interval))
                        ).update(
                                DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME,
                                systemPropertyChange -> systemPropertyChange.changePropertyValue(Long.toString(dataAvailabilityTime))
                        )
                );

        assertThat(changeFuture, willCompleteSuccessfully());
    }
}
