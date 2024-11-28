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

package org.apache.ignite.internal.sql.common.cancel;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.expectQueryCancelled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.common.cancel.api.CancelableOperationType;
import org.apache.ignite.internal.sql.common.cancel.api.OperationCancelHandler;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Cancel sql query using internal API test.
 */
public class ItCancelHandlerRegistryTest extends BaseSqlIntegrationTest {
    @AfterEach
    public void checkResourceLeak() {
        assertThat(txManager().pending(), is(0));
        assertThat(queryProcessor().openedCursors(), is(0));
    }

    @Test
    public void killQueryFromLocal() {
        Ignite node = CLUSTER.aliveNode();

        AsyncSqlCursor<InternalSqlRow> cursor = executeQuery(node, "SELECT 1");

        List<QueryInfo> queries = runningQueries();
        assertThat(queries.size(), is(1));
        UUID targetQueryId = queries.get(0).id();

        OperationCancelHandler handler = handler(node);

        assertThat(handler.cancelAsync(targetQueryId), willBe(true));

        assertThat(runningQueries(), is(empty()));
        expectQueryCancelled(new DrainCursor(cursor));
        assertThat(handler.cancelAsync(targetQueryId), willBe(false));
    }

    @Test
    public void killQueryFromRemote() {
        Ignite local = CLUSTER.node(0);
        Ignite remote = CLUSTER.node(2);

        AsyncSqlCursor<InternalSqlRow> cursor = executeQuery(local, "SELECT 1");

        List<QueryInfo> queries = runningQueries();
        assertThat(queries.size(), is(1));
        UUID targetQueryId = queries.get(0).id();

        OperationCancelHandler remoteHandler = handler(remote);

        assertThat(remoteHandler.cancelAsync(targetQueryId), willBe(true));

        assertThat(runningQueries(), is(empty()));
        expectQueryCancelled(new DrainCursor(cursor));

        assertThat(remoteHandler.cancelAsync(targetQueryId), willBe(false));
        assertThat(handler(local).cancelAsync(targetQueryId), willBe(false));
    }

    private static OperationCancelHandler handler(Ignite node) {
        return unwrapIgniteImpl(node)
                .cancelationManager()
                .handler(CancelableOperationType.SQL_QUERY);
    }

    private static List<QueryInfo> runningQueries() {
        return CLUSTER.runningNodes()
                .flatMap(node -> ((SqlQueryProcessor) unwrapIgniteImpl(node).queryEngine()).runningQueries().stream())
                .collect(Collectors.toList());
    }

    private static AsyncSqlCursor<InternalSqlRow> executeQuery(Ignite node, String query) {
        IgniteImpl ignite = unwrapIgniteImpl(node);

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut = ignite.queryEngine().queryAsync(
                SqlQueryProcessor.DEFAULT_PROPERTIES,
                ignite.observableTimeTracker(),
                null,
                null,
                query
        );

        return await(fut);
    }
}
