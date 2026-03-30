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

package org.apache.ignite.internal.client;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.metastorage.server.WatchListenerInhibitor;
import org.junit.jupiter.api.Test;

class ItClientGetTableSchemaTest extends ClusterPerTestIntegrationTest {

    public static final String TABLE_NAME = "TEST";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void gettingTableSchemaNotYetAvailableOnNodeEventuallyCompletes() throws Exception {
        try (
                IgniteClient client0 = clientConnectedToNode(0);
                IgniteClient client1 = clientConnectedToNode(1)
        ) {
            executeUpdate("CREATE TABLE " + TABLE_NAME + " (ID INT PRIMARY KEY)", client0.sql());
            assertTrue(waitForCondition(() -> client1.tables().table(TABLE_NAME) != null, SECONDS.toMillis(10)));

            ClientTable tableViaClient1 = (ClientTable) client1.tables().table(TABLE_NAME);

            WatchListenerInhibitor.withInhibition(node(1), () -> {
                executeUpdate("ALTER TABLE " + TABLE_NAME + " ADD COLUMN VAL VARCHAR", client0.sql());

                // Node 1 does not have version 2 of the table yet, so the future should hang, but not fail.
                assertThat(tableViaClient1.getSchemaByVersion(2), willTimeoutIn(1, SECONDS));
            });

            assertThat(tableViaClient1.getSchemaByVersion(2), willCompleteSuccessfully());
        }
    }

    private IgniteClient clientConnectedToNode(int nodeIndex) {
        return IgniteClient.builder()
                .addresses("localhost:" + igniteImpl(nodeIndex).clientAddress().port())
                .build();
    }
}
