/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionCancelledException;
import org.apache.ignite.lang.IgniteInternalException;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify running query registry and cancellation.
 */
public class ItRunningQueriesTest extends AbstractBasicIntegrationTest {
    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 10_000;

    /**
     * Execute query with a lot of JOINs to produce very long planning phase. Cancel query on planning phase and check query registry is
     * empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelAtPlanningPhase() throws InterruptedException {
        SqlQueryProcessor queryProcessor = (SqlQueryProcessor) ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
        int cnt = 9;

        for (int i = 0; i < cnt; i++) {
            sql("CREATE TABLE test_tbl" + i + " (id int primary key, val varchar)");
        }

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "test_tbl" + i + " p" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        CompletableFuture<List<List<?>>> fut = CompletableFuture.supplyAsync(() -> sql(sql));

        assertTrue(waitForCondition(
                () -> !queryProcessor.queryRegistry().runningQueries().isEmpty() || fut.isDone(), TIMEOUT_IN_MS));

        Collection<? extends RunningQuery> running = queryProcessor.queryRegistry().runningQueries();

        assertThat(running, iterableWithSize(1));

        RunningQuery qry = first(running);

        assertSame(qry, queryProcessor.queryRegistry().query(qry.id()));

        // Waits for planning.
        assertTrue(waitForCondition(
                () -> qry.state() == QueryState.PLANNING, TIMEOUT_IN_MS));

        qry.cancel();

        assertTrue(waitForCondition(
                () -> queryProcessor.queryRegistry().runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Throwable ex = assertThrows(Throwable.class, fut::get);
        assertTrue(hasCause(ex, IgniteInternalException.class, "The query was cancelled while planning"));
    }

    /**
     * Execute query with a lot of JOINs to produce very long execution phase. Cancel query on execution phase and check query registry is
     * empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelAtExecutionPhase() throws InterruptedException {
        SqlQueryProcessor queryProcessor = (SqlQueryProcessor) ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
        int cnt = 6;

        sql("CREATE TABLE person (id int primary key, val varchar)");

        String data = IntStream.range(0, 1000).mapToObj((i) -> "(" + i + "," + i + ")").collect(joining(", "));
        String insertSql = "INSERT INTO person (id, val) VALUES " + data;

        sql(insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "person p" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        CompletableFuture<List<List<?>>> fut = CompletableFuture.supplyAsync(() -> sql(sql));

        assertTrue(waitForCondition(
                () -> {
                    Collection<? extends RunningQuery> queries = queryProcessor.queryRegistry().runningQueries();

                    return !queries.isEmpty() && first(queries).state() == QueryState.EXECUTING;
                },
                TIMEOUT_IN_MS));

        Collection<? extends RunningQuery> running = queryProcessor.queryRegistry().runningQueries();

        assertEquals(1, running.size());

        RunningQuery qry = first(running);

        assertSame(qry, queryProcessor.queryRegistry().query(qry.id()));

        qry.cancel();

        assertTrue(waitForCondition(
                () -> queryProcessor.queryRegistry().runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Throwable ex = assertThrows(Throwable.class, fut::get);
        assertTrue(hasCause(ex, IgniteInternalException.class, "The query was cancelled"));
    }

    /**
     * Execute query with a lot of JOINs to produce very long excution phase. Cancel query on execution phase on remote node (no query
     * originator node) and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelByRemoteFragment() throws InterruptedException {
        SqlQueryProcessor queryProcessor = (SqlQueryProcessor) ((IgniteImpl) CLUSTER_NODES.get(1)).queryEngine();
        int cnt = 6;

        sql("CREATE TABLE t (id int primary key, val varchar)");

        String data = IntStream.range(0, 10000).mapToObj((i) -> "(" + i + ",'" + i + "')").collect(joining(", "));
        String insertSql = "INSERT INTO t (id, val) VALUES " + data;

        sql(insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "t t" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        CompletableFuture<List<List<?>>> fut = CompletableFuture.supplyAsync(() -> sql(sql));

        assertTrue(waitForCondition(
                () -> {
                    Collection<? extends RunningQuery> queries = queryProcessor.queryRegistry().runningQueries();

                    return !queries.isEmpty() && first(queries).state() == QueryState.EXECUTING;
                },
                TIMEOUT_IN_MS));

        Collection<? extends RunningQuery> running = queryProcessor.queryRegistry().runningQueries();
        RunningQuery qry = first(running);

        assertSame(qry, queryProcessor.queryRegistry().query(qry.id()));

        qry.cancel();

        assertTrue(waitForCondition(
                () -> queryProcessor.queryRegistry().runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Throwable ex = assertThrows(Throwable.class, fut::get);
        assertTrue(hasCause(ex, ExecutionCancelledException.class, null));
    }
}
