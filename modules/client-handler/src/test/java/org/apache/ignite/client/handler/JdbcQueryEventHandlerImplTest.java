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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.handler.JdbcQueryEventHandlerImpl.JdbcConnectionContext;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.JdbcStatementType;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcConnectResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
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
    private IgniteTransactionsImpl igniteTransactions;

    private ClientResourceRegistry resourceRegistry;

    private JdbcQueryEventHandler eventHandler;

    @BeforeEach
    public void setUp() {
        resourceRegistry = new ClientResourceRegistry();

        eventHandler = new JdbcQueryEventHandlerImpl(
                queryProcessor,
                catalog,
                resourceRegistry,
                igniteTransactions
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
        assertThat(result.hasResults(), is(false));
        assertThat(result.err(), containsString("Unable to connect"));
    }

    @Test
    public void contextClosedDuringBatchQuery() throws Exception {
        int timeout = 30;
        CountDownLatch startTxLatch = new CountDownLatch(1);
        CountDownLatch registryCloseLatch = new CountDownLatch(1);
        long connectionId = acquireConnectionId();

        when(igniteTransactions.begin()).thenAnswer(v -> {
            registryCloseLatch.countDown();
            assertThat(startTxLatch.await(timeout, TimeUnit.SECONDS), is(true));

            return null;
        });

        CompletableFuture<JdbcBatchExecuteResult> fut = IgniteTestUtils.runAsync(() ->
                eventHandler.batchAsync(connectionId, new JdbcBatchExecuteRequest("x", List.of("QUERY"), false)).get()
        );

        assertThat(registryCloseLatch.await(timeout, TimeUnit.SECONDS), is(true));
        resourceRegistry.close();

        startTxLatch.countDown();

        JdbcBatchExecuteResult res = fut.get();

        assertThat(res.status(), is(STATUS_FAILED));
        assertThat(res.hasResults(), is(false));
        assertThat(res.err(), containsString("Connection is closed"));
    }

    @Test
    public void explicitTxRollbackOnCloseRegistry() {
        InternalTransaction tx = mock(InternalTransaction.class);

        when(tx.rollbackAsync()).thenReturn(nullCompletedFuture());
        when(igniteTransactions.begin()).thenReturn(tx);

        long connectionId = acquireConnectionId();

        await(eventHandler.batchAsync(connectionId, new JdbcBatchExecuteRequest("schema", List.of("UPDATE 1"), false)));

        verify(igniteTransactions).begin();
        verify(tx, times(0)).rollbackAsync();

        resourceRegistry.close();
        verify(tx, times(1)).rollbackAsync();
        verifyNoMoreInteractions(tx);
    }

    @Test
    public void singleTxUsedForMultipleOperations() {
        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Expected")));

        InternalTransaction tx = mock(InternalTransaction.class);
        when(tx.commitAsync()).thenReturn(nullCompletedFuture());
        when(tx.rollbackAsync()).thenReturn(nullCompletedFuture());
        when(igniteTransactions.begin()).thenReturn(tx);

        long connectionId = acquireConnectionId();
        verify(igniteTransactions, times(0)).begin();

        String schema = "schema";
        JdbcStatementType type = JdbcStatementType.SELECT_STATEMENT_TYPE;

        await(eventHandler.queryAsync(connectionId, new JdbcQueryExecuteRequest(type, schema, 1024, 1, "SELECT 1", null, false, false)));
        verify(igniteTransactions, times(1)).begin();
        await(eventHandler.batchAsync(connectionId, new JdbcBatchExecuteRequest("schema", List.of("UPDATE 1", "UPDATE 2"), false)));
        verify(igniteTransactions, times(1)).begin();

        await(eventHandler.finishTxAsync(connectionId, false));
        verify(tx).rollbackAsync();

        await(eventHandler.batchAsync(connectionId, new JdbcBatchExecuteRequest("schema", List.of("UPDATE 1", "UPDATE 2"), false)));
        verify(igniteTransactions, times(2)).begin();
        await(eventHandler.queryAsync(connectionId, new JdbcQueryExecuteRequest(type, schema, 1024, 1, "SELECT 2", null, false, false)));
        verify(igniteTransactions, times(2)).begin();
        await(eventHandler.batchAsync(connectionId, new JdbcBatchExecuteRequest("schema", List.of("UPDATE 3", "UPDATE 4"), false)));
        verify(igniteTransactions, times(2)).begin();

        await(eventHandler.finishTxAsync(connectionId, true));
        verify(tx).commitAsync();

        verify(igniteTransactions, times(5)).observableTimestampTracker();
        verifyNoMoreInteractions(igniteTransactions);
        verify(queryProcessor, times(5)).queryAsync(any(), any(), any(), any(), any(Object[].class));
    }

    private long acquireConnectionId() {
        JdbcConnectResult result = await(eventHandler.connect(ZoneId.systemDefault()));

        assertThat(result, notNullValue());
        assertThat(result.status(), is(STATUS_SUCCESS));
        assertThat(result.hasResults(), is(true));

        return result.connectionId();
    }
}
