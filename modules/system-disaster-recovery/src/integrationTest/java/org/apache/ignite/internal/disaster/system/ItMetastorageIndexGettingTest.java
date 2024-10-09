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

package org.apache.ignite.internal.disaster.system;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairResponse;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;

class ItMetastorageIndexGettingTest extends ClusterPerTestIntegrationTest {
    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();

    @Override
    protected int initialNodes() {
        return 0;
    }

    @Test
    void indexAndTermAreReturnedFromRunningNode() throws Exception {
        cluster.startAndInit(2, parametersBuilder -> {
            parametersBuilder.cmgNodeNames(cluster.nodeName(0));
            parametersBuilder.metaStorageNodeNames(cluster.nodeName(1));
        });

        StartMetastorageRepairResponse response = getMetastorageIndexAndTermFrom(1);

        assertThat(response.raftIndex(), is(greaterThanOrEqualTo(1L)));
        assertThat(response.raftTerm(), is(greaterThanOrEqualTo(1L)));
    }

    private StartMetastorageRepairResponse getMetastorageIndexAndTermFrom(int targetNodeIndex)
            throws InterruptedException, ExecutionException, TimeoutException {
        IgniteImpl ignite0 = igniteImpl(0);

        CompletableFuture<NetworkMessage> future = ignite0.clusterService().messagingService().invoke(
                cluster.nodeName(targetNodeIndex),
                messagesFactory.startMetastorageRepairRequest().build(),
                SECONDS.toMillis(10)
        );
        return (StartMetastorageRepairResponse) future.get(10, SECONDS);
    }

    @Test
    void indexAndTermAreReturnedFromNodeThatHangsOnStartingMetastorage() throws Exception {
        cluster.startAndInit(3, parametersBuilder -> {
            parametersBuilder.cmgNodeNames(cluster.nodeName(0));
            parametersBuilder.metaStorageNodeNames(cluster.nodeName(1));
        });

        // Stopping node 1 will make Metastorage lose majority.
        IntStream.of(1, 2).parallel().forEach(cluster::stopNode);

        // This will not be able to finish its startup.
        cluster.startEmbeddedNode(2);

        StartMetastorageRepairResponse response = getMetastorageIndexAndTermFrom(2);

        assertThat(response.raftIndex(), is(greaterThanOrEqualTo(1L)));
        assertThat(response.raftTerm(), is(greaterThanOrEqualTo(1L)));
    }
}
