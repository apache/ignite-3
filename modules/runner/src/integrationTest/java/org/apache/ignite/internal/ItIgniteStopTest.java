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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import com.typesafe.config.parser.ConfigDocument;
import com.typesafe.config.parser.ConfigDocumentFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;

class ItIgniteStopTest extends ClusterPerTestIntegrationTest {
    private static final long RAFT_RETRY_TIMEOUT_MILLIS = 15000;

    @Override
    protected int initialNodes() {
        return 0;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        ConfigDocument document = ConfigDocumentFactory.parseString(super.getNodeBootstrapConfigTemplate())
                .withValueText("ignite.raft.retryTimeoutMillis", Long.toString(RAFT_RETRY_TIMEOUT_MILLIS));
        return document.render();
    }

    @Test
    void testNodesCouldBeStoppedEvenIfMetastorageIsUnavailable() throws InterruptedException {
        int nodeCount = 3;

        cluster.startAndInit(nodeCount, builder -> {
            builder.cmgNodeNames(cluster.nodeName(2));
            builder.metaStorageNodeNames(cluster.nodeName(0));
        });

        Ignite node1 = cluster.node(1);

        node1.sql().executeScript("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL VARCHAR)");

        KeyValueView<Integer, String> kvView = node1.tables().table("TEST").keyValueView(Integer.class, String.class);

        kvView.put(null, 1, "one");

        // Stop the single meta storage node.
        cluster.stopNode(0);

        // Imitate some activity on cluster
        kvView.putAsync(null, 2, "two");

        assertThat(cluster.stopNodeAsync(1), willCompleteSuccessfully());

        assertThat(cluster.stopNodeAsync(2), willCompleteSuccessfully());
    }
}
