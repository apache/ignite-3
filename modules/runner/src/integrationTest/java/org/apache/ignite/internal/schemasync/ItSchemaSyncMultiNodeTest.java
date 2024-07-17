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

package org.apache.ignite.internal.schemasync;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.test.WatchListenerInhibitor.metastorageEventsInhibitor;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests about basic Schema Synchronization properties that should be tested using a few Ignite node.
 */
class ItSchemaSyncMultiNodeTest extends ClusterPerTestIntegrationTest {
    private static final int NODES_TO_START = 2;

    private static final int NODE_0_INDEX = 0;
    private static final int NODE_1_INDEX = 1;

    private static final String TABLE_NAME = "test";

    @Override
    protected int initialNodes() {
        return NODES_TO_START;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{ 0 };
    }

    /**
     * Makes sure that, if a DDL is executed on one node, its future is waited out, then a SQL DML is executed
     * on another node of the cluster, that DML sees the results of the DDL (i.e. it cannot see the old schema).
     */
    @Test
    void sqlDmlAfterDdlOnAnotherNodeSeesDdlResults() {
        createTableAtNode(NODE_0_INDEX);

        assertDoesNotThrow(() -> executeSql(NODE_1_INDEX, "INSERT INTO " + TABLE_NAME + " (id, val) VALUES (1, 'one')"));

        assertThat(executeSql(NODE_1_INDEX, "SELECT val FROM " + TABLE_NAME + " WHERE id = 1"), is(List.of(List.of("one"))));
    }

    /**
     * Makes sure that, if a DDL is executed on one node, its future is waited out, then a SQL DML is executed
     * on another node of the cluster, that DML sees the results of the DDL (i.e. it cannot see the old schema).
     *
     * <p>This particular scenario uses metastorage inhibiting to make sure that schema sync is not missed.
     */
    @Test
    void sqlDmlAfterDdlOnAnotherNodeSeesDdlResultsWithInhibitor() {
        WatchListenerInhibitor inhibitorOnNode1 = metastorageEventsInhibitor(node(NODE_1_INDEX));
        inhibitorOnNode1.startInhibit();

        try {
            createTableAtNode(NODE_0_INDEX);

            CompletableFuture<?> insertFuture = runAsync(
                    () -> executeSql(NODE_1_INDEX, "INSERT INTO " + TABLE_NAME + " (id, val) VALUES (1, 'one')")
            );

            assertThat(insertFuture, willTimeoutIn(100, MILLISECONDS));

            inhibitorOnNode1.stopInhibit();

            assertThat(insertFuture, willCompleteSuccessfully());

            assertThat(executeSql(NODE_1_INDEX, "SELECT val FROM " + TABLE_NAME + " WHERE id = 1"), is(List.of(List.of("one"))));
        } finally {
            inhibitorOnNode1.stopInhibit();
        }
    }

    private void createTableAtNode(int nodeIndex) {
        cluster.doInSession(nodeIndex, session -> {
            executeUpdate("CREATE TABLE " + TABLE_NAME + " (id int PRIMARY KEY, val varchar)", session);
        });
    }

    /**
     * Makes sure that, if a DDL is executed on one node, its future is waited out, then a KV operation is executed
     * on another node of the cluster, that KV operation sees the results of the DDL (i.e. it cannot see the old schema).
     */
    @Test
    void kvDmlAfterDdlOnAnotherNodeSeesDdlResults() {
        createTableAtNode(NODE_0_INDEX);

        Table tableAtNode1 = node(NODE_1_INDEX).tables().table(TABLE_NAME);

        addColumnAtNode(NODE_0_INDEX);

        Tuple key = Tuple.create().set("id", 1);
        assertDoesNotThrow(() -> tableAtNode1.keyValueView().put(null, key, Tuple.create().set("added", 42)));

        Tuple retrievedTuple = tableAtNode1.keyValueView().get(null, key);
        assertThat(retrievedTuple, is(notNullValue()));
        assertThat(retrievedTuple.value("added"), is(42));
    }

    /**
     * Makes sure that, if a DDL is executed on one node, its future is waited out, then a KV operation is executed
     * on another node of the cluster, that KV operation sees the results of the DDL (i.e. it cannot see the old schema).
     *
     * <p>This particular scenario uses metastorage inhibiting to make sure that schema sync is not missed.
     */
    @Test
    void kvDmlAfterDdlOnAnotherNodeSeesDdlResultsWithInhibitor() {
        createTableAtNode(NODE_0_INDEX);

        Table tableAtNode1 = node(NODE_1_INDEX).tables().table(TABLE_NAME);
        KeyValueView<Tuple, Tuple> kvViewAtNode1 = tableAtNode1.keyValueView();

        Tuple key = Tuple.create().set("id", 1);

        WatchListenerInhibitor inhibitorOnNode1 = metastorageEventsInhibitor(node(NODE_1_INDEX));
        inhibitorOnNode1.startInhibit();

        try {
            addColumnAtNode(NODE_0_INDEX);

            CompletableFuture<?> putFuture = kvViewAtNode1.putAsync(null, key, Tuple.create().set("added", 42));

            assertThat(putFuture, willTimeoutIn(100, MILLISECONDS));

            inhibitorOnNode1.stopInhibit();

            assertThat(putFuture, willCompleteSuccessfully());
        } finally {
            inhibitorOnNode1.stopInhibit();
        }

        Tuple retrievedTuple = kvViewAtNode1.get(null, key);
        assertThat(retrievedTuple, is(notNullValue()));
        assertThat(retrievedTuple.value("added"), is(42));
    }

    private void addColumnAtNode(int nodeIndex) {
        cluster.doInSession(nodeIndex, session -> {
            executeUpdate("ALTER TABLE " + TABLE_NAME + " ADD COLUMN added int", session);
        });
    }

    /**
     * Makes sure that, if a DDL is executed on one node, its future is waited out, then another DDL is executed
     * on another node of the cluster, that second DDL sees the results of the first DDL (i.e. it cannot see the old schema).
     */
    @Test
    void ddlAfterDdlOnAnotherNodeSeesFirstDdlResults() {
        WatchListenerInhibitor inhibitorOnNode1 = metastorageEventsInhibitor(node(NODE_1_INDEX));
        inhibitorOnNode1.startInhibit();

        try {
            cluster.doInSession(NODE_0_INDEX, session -> {
                executeUpdate("CREATE ZONE test_zone WITH STORAGE_PROFILES='default'", session);
            });

            CompletableFuture<Void> tableCreationFuture = runAsync(() -> cluster.doInSession(NODE_1_INDEX, session -> {
                executeUpdate("CREATE TABLE " + TABLE_NAME + " (id int PRIMARY KEY, val varchar) WITH primary_zone='TEST_ZONE'", session);
            }));

            assertThat(tableCreationFuture, willTimeoutIn(1, SECONDS));

            inhibitorOnNode1.stopInhibit();

            assertThat(tableCreationFuture, willCompleteSuccessfully());
        } finally {
            inhibitorOnNode1.stopInhibit();
        }
    }
}
