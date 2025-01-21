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

package org.apache.ignite.client.handler;

import static org.apache.ignite.internal.jdbc.proto.event.Response.STATUS_FAILED;
import static org.apache.ignite.internal.jdbc.proto.event.Response.STATUS_SUCCESS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.ignite.client.handler.JdbcQueryEventHandlerImpl.JdbcConnectionContext;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.JdbcStatementType;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcConnectResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.lang.CancellationToken;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test to verify {@link JdbcQueryEventHandlerImpl}.
 */
@ExtendWith(MockitoExtension.class)
class JdbcQueryEventHandlerImplTest extends BaseIgniteAbstractTest {

    @Mock
    private QueryProcessor queryProcessor;

    @Mock
    private JdbcMetadataCatalog catalog;

    @Mock
    private TxManager txManager;

    private ClientResourceRegistry resourceRegistry;

    private JdbcQueryEventHandler eventHandler;

    @BeforeEach
    public void setUp() {
        resourceRegistry = new ClientResourceRegistry();

        eventHandler = new JdbcQueryEventHandlerImpl(
                queryProcessor,
                catalog,
                resourceRegistry,
                txManager
        );
    }

    @Test
    void connect() throws IgniteInternalCheckedException {
        long connectionId = acquireConnectionId();

        JdbcConnectionContext context = resourceRegistry.get(connectionId).get(JdbcConnectionContext.class);
        assertThat(context, notNullValue());

        verifyNoInteractions(queryProcessor);
    }

    @Test
    void connectOnStoppingNode() {
        resourceRegistry.close();

        JdbcConnectResult result = await(eventHandler.connect(ZoneId.systemDefault()));

        assertThat(result, notNullValue());
        assertThat(result.status(), is(STATUS_FAILED));
        assertThat(result.success(), is(false));
        assertThat(result.err(), containsString("Unable to connect"));
    }

    @Test
    public void contextClosedDuringBatchQuery() throws Exception {
        int timeout = 30;
        CountDownLatch startTxLatch = new CountDownLatch(1);
        CountDownLatch registryCloseLatch = new CountDownLatch(1);
        long connectionId = acquireConnectionId();

        when(txManager.beginExplicitRw(any(), any())).thenAnswer(v -> {
            registryCloseLatch.countDown();
            assertThat(startTxLatch.await(timeout, TimeUnit.SECONDS), is(true));

            return null;
        });

        CompletableFuture<JdbcBatchExecuteResult> fut = IgniteTestUtils.runAsync(() ->
                eventHandler.batchAsync(connectionId, createExecuteBatchRequest("x", "QUERY")).get()
        );

        assertThat(registryCloseLatch.await(timeout, TimeUnit.SECONDS), is(true));
        resourceRegistry.close();

        startTxLatch.countDown();

        JdbcBatchExecuteResult res = fut.get();

        assertThat(res.status(), is(STATUS_FAILED));
        assertThat(res.success(), is(false));
        assertThat(res.err(), containsString("Connection is closed"));
    }

    @Test
    public void explicitTxRollbackOnCloseRegistry() {
        InternalTransaction tx = mock(InternalTransaction.class);

        when(tx.rollbackAsync()).thenReturn(nullCompletedFuture());
        when(txManager.beginExplicitRw(any(), any())).thenReturn(tx);

        long connectionId = acquireConnectionId();

        await(eventHandler.batchAsync(connectionId, createExecuteBatchRequest("x", "UPDATE 1")));

        verify(txManager).beginExplicitRw(any(), any());
        verify(tx, times(0)).rollbackAsync();

        resourceRegistry.close();
        verify(tx, times(1)).rollbackAsync();
        verifyNoMoreInteractions(tx);
    }

    @Test
    public void singleTxUsedForMultipleOperations() {
        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Expected")));

        InternalTransaction tx = mock(InternalTransaction.class);
        when(tx.commitAsync()).thenReturn(nullCompletedFuture());
        when(tx.rollbackAsync()).thenReturn(nullCompletedFuture());
        when(txManager.beginExplicitRw(any(), any())).thenReturn(tx);

        long connectionId = acquireConnectionId();
        verify(txManager, times(0)).beginExplicitRw(any(), any());

        String schema = "schema";
        JdbcStatementType type = JdbcStatementType.SELECT_STATEMENT_TYPE;

        await(eventHandler.queryAsync(
                connectionId, createExecuteRequest(schema, "SELECT 1", type)
        ));
        verify(txManager, times(1)).beginExplicitRw(any(), any());
        await(eventHandler.batchAsync(connectionId, createExecuteBatchRequest("schema", "UPDATE 1", "UPDATE 2")));
        verify(txManager, times(1)).beginExplicitRw(any(), any());

        await(eventHandler.finishTxAsync(connectionId, false));
        verify(tx).rollbackAsync();

        await(eventHandler.batchAsync(connectionId, createExecuteBatchRequest("schema", "UPDATE 1", "UPDATE 2")));
        verify(txManager, times(2)).beginExplicitRw(any(), any());
        await(eventHandler.queryAsync(
                connectionId, createExecuteRequest(schema, "SELECT 2", type)
        ));
        verify(txManager, times(2)).beginExplicitRw(any(), any());
        await(eventHandler.batchAsync(connectionId, createExecuteBatchRequest("schema", "UPDATE 3", "UPDATE 4")));
        verify(txManager, times(2)).beginExplicitRw(any(), any());

        await(eventHandler.finishTxAsync(connectionId, true));
        verify(tx).commitAsync();

        verifyNoMoreInteractions(txManager);
        verify(queryProcessor, times(5)).queryAsync(any(), any(), any(), any(), any(), any(Object[].class));
    }

    @Test
    void simpleQueryCancellation() {
        setupMockForCancellation();

        long connectionId = acquireConnectionId();

        JdbcQueryExecuteRequest executeRequest = createExecuteRequest("schema", "SELECT 1", JdbcStatementType.SELECT_STATEMENT_TYPE);

        CompletableFuture<? extends Response> resultFuture = eventHandler.queryAsync(connectionId, executeRequest);

        assertFalse(resultFuture.isDone(), "Future must not be completed until cancellation request is sent");

        cancelAndVerify(connectionId, executeRequest.correlationToken(), resultFuture);
    }

    @Test
    void batchedQueryCancellation() {
        setupMockForCancellation();

        long connectionId = acquireConnectionId();

        JdbcBatchExecuteRequest executeRequest = createExecuteBatchRequest("schema", "SELECT 1");

        CompletableFuture<? extends Response> resultFuture = eventHandler.batchAsync(connectionId, executeRequest);

        assertFalse(resultFuture.isDone(), "Future must not be completed until cancellation request is sent");

        cancelAndVerify(connectionId, executeRequest.correlationToken(), resultFuture);
    }

    @Test
    void prepBatchedQueryCancellation() {
        setupMockForCancellation();

        long connectionId = acquireConnectionId();

        JdbcBatchPreparedStmntRequest executeRequest = createExecutePrepBatchRequest("schema", "SELECT 1");

        CompletableFuture<? extends Response> resultFuture = eventHandler.batchPrepStatementAsync(connectionId, executeRequest);

        assertFalse(resultFuture.isDone(), "Future must not be completed until cancellation request is sent");

        cancelAndVerify(connectionId, executeRequest.correlationToken(), resultFuture);
    }

    private long acquireConnectionId() {
        JdbcConnectResult result = await(eventHandler.connect(ZoneId.systemDefault()));

        assertThat(result, notNullValue());
        assertThat(result.status(), is(STATUS_SUCCESS));
        assertThat(result.success(), is(true));

        return result.connectionId();
    }

    private void setupMockForCancellation() {
        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(), any(Object[].class)))
                .thenAnswer(invocation -> {
                    CancellationToken token = invocation.getArgument(3);

                    CompletableFuture<AsyncSqlCursor<InternalSqlRow>> result = new CompletableFuture<>();
                    CancelHandleHelper.addCancelAction(token, () -> result.completeExceptionally(new QueryCancelledException()), result);

                    return result;
                });
    }

    private void cancelAndVerify(long connectionId, long token, CompletableFuture<? extends Response> resultFuture) {
        assertThat(
                eventHandler.cancelAsync(connectionId, token),
                willSucceedFast()
        );

        assertThat(
                resultFuture,
                willBe(responseThat(
                        "response with error message",
                        r -> r.err().startsWith("The query was cancelled while executing")
                ))
        );
    }

    private static JdbcQueryExecuteRequest createExecuteRequest(String schema, String query, JdbcStatementType type) {
        //noinspection DataFlowIssue
        return new JdbcQueryExecuteRequest(
                type, schema, 1024, 1, query, null, false, false, 0, System.currentTimeMillis()
        );
    }

    private static JdbcBatchExecuteRequest createExecuteBatchRequest(String schema, String... statements) {
        return new JdbcBatchExecuteRequest(
                schema, List.of(statements), false, 0, System.currentTimeMillis()
        );
    }

    private static JdbcBatchPreparedStmntRequest createExecutePrepBatchRequest(String schema, String query) {
        return new JdbcBatchPreparedStmntRequest(
                schema, query, Collections.singletonList(new Object[] {1}), false, 0, System.currentTimeMillis()
        );
    }

    private static Matcher<Response> responseThat(String description, Predicate<Response> checker) {
        return new CustomMatcher<>(description) {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof Response && checker.test((Response) actual) == Boolean.TRUE;
            }
        };
    }
}
