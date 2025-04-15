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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.server.AbstractKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.WatchProcessor;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.util.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class ItMetaStorageMaintenanceTest extends ItMetaStorageMultipleNodesAbstractTest {
    @Test
    void initiateForcefulVotersChangeMakesNodeLeaderForcefully() throws Exception {
        start3VotingNodes();

        Node node0 = nodes.get(0);

        // Metastorage works.
        assertThatMetastorageHasMajority(node0);

        // Stop the majority.
        stopAllNodesExcept0();

        // Metastorage does not work anymore.
        assertThatMetastorageHasNoMajority(node0);

        node0.metaStorageManager.initiateForcefulVotersChange(1, allNodeNames());

        assertThatMetastorageHasMajority(node0);
    }

    private Set<String> allNodeNames() {
        return nodes.stream().map(Node::name).collect(toSet());
    }

    /**
     * Starts 3 voting Raft nodes. 'Voting' here is opposed to a 'learner' node which does not vote.
     *
     * @throws NodeStoppingException If a node is being stopped.
     */
    private void start3VotingNodes() throws NodeStoppingException {
        Node node0 = startNode();
        Node node1 = startNode();
        Node node2 = startNode();

        node0.cmgManager.initCluster(List.of(node0.name(), node1.name(), node2.name()), List.of(node0.name()), "test");

        assertThat(
                allOf(node0.cmgManager.onJoinReady(), node1.cmgManager.onJoinReady(), node2.cmgManager.onJoinReady()),
                willCompleteSuccessfully()
        );

        startMetastorageOn(List.of(node0, node1, node2));

        node0.waitWatches();
        node1.waitWatches();
        node2.waitWatches();
    }

    private static void assertThatMetastorageHasNoMajority(Node node0) {
        assertThat(node0.metaStorageManager.get(new ByteArray("abc")), willTimeoutIn(1, SECONDS));
    }

    private static void assertThatMetastorageHasMajority(Node node0) {
        assertThat(node0.metaStorageManager.get(new ByteArray("abc")), willCompleteSuccessfully());
    }

    private void stopAllNodesExcept0() {
        for (int i = 1; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            node.clusterService.beforeNodeStop();
            assertThat(node.clusterService.stopAsync(), willCompleteSuccessfully());
        }
    }

    @Test
    void initiateForcefulVotersChangeStopsLearnerManagementIfPauseRequested() throws Exception {
        start3VotingNodes();

        Node node0 = nodes.get(0);

        // Stop the majority.
        stopAllNodesExcept0();

        node0.metaStorageManager.initiateForcefulVotersChange(1, allNodeNames());

        Node node3 = startNode();

        // Make sure the leader does not manage learners (as we requested it to pause secondary duties).
        assertFalse(
                waitForCondition(() -> learnersAt(node0).contains(node3.name()), SECONDS.toMillis(3)),
                "The leader still manages learners"
        );
    }

    @Test
    void initiateForcefulVotersChangeContinuesLearnerManagementIfPauseNotRequested() throws Exception {
        start3VotingNodes();

        Node node0 = nodes.get(0);

        // Stop the majority.
        stopAllNodesExcept0();

        node0.metaStorageManager.initiateForcefulVotersChange(1, Set.of(node0.name()));

        Node node3 = startNode();

        // Make sure the leader manages learners (as we requested it NOT to pause secondary duties).
        assertTrue(
                waitForCondition(() -> learnersAt(node0).contains(node3.name()), SECONDS.toMillis(10)),
                "The leader does not manage learners"
        );
    }

    private static Set<String> learnersAt(Node node0) {
        CompletableFuture<Set<String>> future = node0.getMetaStorageLearners();

        assertThat(future, willCompleteSuccessfully());

        return future.join();
    }

    @Test
    void initiateForcefulVotersChangeStopsIdleSafeTimePropagationIfPauseRequested() throws Exception {
        enableIdleSafeTimeSync();
        start3VotingNodes();

        Node node0 = nodes.get(0);

        // Stop the majority.
        stopAllNodesExcept0();

        node0.metaStorageManager.initiateForcefulVotersChange(1, allNodeNames());

        ClusterTime clusterTime0 = node0.metaStorageManager.clusterTime();

        causeSafeTimeCommandsIssuedBeforePausingToBeApplied(node0);

        // Make sure the leader does not propagate Metastorage SafeTime (as we requested it to pause secondary duties).
        HybridTimestamp timeAtStart = clusterTime0.currentSafeTime();
        assertFalse(
                waitForCondition(() -> clusterTime0.currentSafeTime().longValue() > timeAtStart.longValue(), SECONDS.toMillis(2)),
                () -> "The leader still propagates safetime " + clusterTime0.currentSafeTime()
        );
    }

    private static void causeSafeTimeCommandsIssuedBeforePausingToBeApplied(Node node) {
        // We execute a PUT command and then wait for SafeTime to be advanced. This guarantees that idle SafeTime propagation
        // commands before pausing idle SafeTime propagation get executed and we don't get a non-relevant test failure.
        assertThat(node.metaStorageManager.put(new ByteArray("abc"), ArrayUtils.BYTE_EMPTY_ARRAY), willCompleteSuccessfully());

        // TODO: IGNITE-15723 After a component factory is implemented, need to get rid of reflection here.
        var storage = (SimpleInMemoryKeyValueStorage) getFieldValue(node.metaStorageManager, MetaStorageManagerImpl.class, "storage");
        var watchProcessor = (WatchProcessor) getFieldValue(storage, AbstractKeyValueStorage.class, "watchProcessor");

        CompletableFuture<Void> notificationFuture = getFieldValue(watchProcessor, WatchProcessor.class, "notificationFuture");
        if (notificationFuture != null) {
            assertThat(notificationFuture, willCompleteSuccessfully());
        }
    }

    @Test
    void initiateForcefulVotersChangeKeepsIdleSafeTimePropagationIfPauseNotRequested() throws Exception {
        enableIdleSafeTimeSync();
        start3VotingNodes();

        Node node0 = nodes.get(0);

        // Stop the majority.
        stopAllNodesExcept0();

        node0.metaStorageManager.initiateForcefulVotersChange(1, Set.of(node0.name()));

        ClusterTime clusterTime0 = node0.metaStorageManager.clusterTime();
        HybridTimestamp timeBeforeOp = clusterTime0.currentSafeTime();

        // Make sure the leader propagates Metastorage SafeTime (as we requested it NOT to pause secondary duties).
        assertTrue(
                waitForCondition(() -> clusterTime0.currentSafeTime().longValue() > timeBeforeOp.longValue(), SECONDS.toMillis(10)),
                "The leader does not propagate safetime"
        );
    }
}
