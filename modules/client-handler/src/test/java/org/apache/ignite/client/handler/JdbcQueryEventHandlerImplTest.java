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
import static org.apache.ignite.internal.sql.api.SessionImpl.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.client.handler.JdbcQueryEventHandlerImpl.JdbcConnectionContext;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.JdbcStatementType;
import org.apache.ignite.internal.jdbc.proto.event.JdbcConnectResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test to verify {@link JdbcQueryEventHandlerImpl}.
 */
@ExtendWith(MockitoExtension.class)
class JdbcQueryEventHandlerImplTest {

    @Mock
    private QueryProcessor queryProcessor;

    @Mock
    private JdbcMetadataCatalog catalog;

    private ClientResourceRegistry resourceRegistry;

    private JdbcQueryEventHandler eventHandler;

    @BeforeEach
    public void setUp() {
        resourceRegistry = new ClientResourceRegistry();

        eventHandler = new JdbcQueryEventHandlerImpl(
                queryProcessor,
                catalog,
                resourceRegistry
        );
    }

    @Test
    void connect() throws IgniteInternalCheckedException {
        long connectionId = acquireConnectionId();

        JdbcConnectionContext context = resourceRegistry.get(connectionId).get(JdbcConnectionContext.class);
        assertThat(context, notNullValue());

        verifyNoInteractions(queryProcessor);

        SessionId expectedSessionId = new SessionId(UUID.randomUUID());

        when(queryProcessor.createSession(any())).thenReturn(expectedSessionId);

        await(context.doInSession(actualSessionId -> {
            assertThat(actualSessionId, is(expectedSessionId));

            return CompletableFuture.completedFuture(null);
        }));

        verify(queryProcessor, only()).createSession(any());
        verifyNoMoreInteractions(queryProcessor);
    }

    @Test
    public void sessionRecreatedIfExpired() throws IgniteInternalCheckedException {
        long connectionId = acquireConnectionId();

        JdbcConnectionContext context = resourceRegistry.get(connectionId).get(JdbcConnectionContext.class);

        when(queryProcessor.createSession(any())).thenAnswer(inv -> new SessionId(UUID.randomUUID()));

        List<Integer> errorCodes = List.of(Sql.SESSION_EXPIRED_ERR, Sql.SESSION_NOT_FOUND_ERR);

        for (int errorCode : errorCodes) {
            AtomicBoolean shouldThrow = new AtomicBoolean(true);

            await(context.doInSession(sessionId -> {
                if (shouldThrow.compareAndSet(true, false)) {
                    return CompletableFuture.failedFuture(new IgniteInternalException(errorCode));
                }

                return CompletableFuture.completedFuture(null);
            }));
        }

        verify(queryProcessor, times(errorCodes.size() + 1 /* initial session */)).createSession(any());
        verifyNoMoreInteractions(queryProcessor);
    }

    @Test
    public void sessionRecreatedOnlyOnce() throws IgniteInternalCheckedException {
        long connectionId = acquireConnectionId();

        JdbcConnectionContext context = resourceRegistry.get(connectionId).get(JdbcConnectionContext.class);

        when(queryProcessor.createSession(any())).thenAnswer(inv -> new SessionId(UUID.randomUUID()));

        CompletableFuture<?> result = context.doInSession(
                sessionId -> CompletableFuture.failedFuture(new IgniteInternalException(Sql.SESSION_EXPIRED_ERR))
        );

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(true));

        verify(queryProcessor, times(2 /* initial session + recreation */)).createSession(any());
        verifyNoMoreInteractions(queryProcessor);
    }

    @Test
    void connectOnStoppingNode() {
        resourceRegistry.close();

        JdbcConnectResult result = await(eventHandler.connect());

        assertThat(result, notNullValue());
        assertThat(result.status(), is(STATUS_FAILED));
        assertThat(result.hasResults(), is(false));
        assertThat(result.err(), containsString("Unable to connect"));
    }

    @Test
    void scriptsAreNotSupported() {
        long connectionId = acquireConnectionId();

        Response response = await(eventHandler.queryAsync(connectionId, new JdbcQueryExecuteRequest(
                JdbcStatementType.ANY_STATEMENT_TYPE, "my_schema", 1024, 1024, "SELECT 1", ArrayUtils.OBJECT_EMPTY_ARRAY
        )));

        assertThat(response, notNullValue());
        assertThat(response.status(), is(STATUS_FAILED));
        assertThat(response.hasResults(), is(false));
        assertThat(response.err(), containsString("Scripts are not supported"));
    }

    @Test
    void sessionIdIsUsedForQueryExecution() {
        SessionId expectedSessionId = new SessionId(UUID.randomUUID());

        when(queryProcessor.createSession(any())).thenReturn(expectedSessionId);

        when(queryProcessor.querySingleAsync(eq(expectedSessionId), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("This is fine")));

        long connectionId = acquireConnectionId();

        await(eventHandler.queryAsync(connectionId, new JdbcQueryExecuteRequest(
                JdbcStatementType.SELECT_STATEMENT_TYPE, "my_schema", 1024, 1024, "SELECT 1", ArrayUtils.OBJECT_EMPTY_ARRAY
        )));

        verify(queryProcessor).createSession(any());
        verify(queryProcessor).querySingleAsync(eq(expectedSessionId), any(), any(), any());
        verifyNoMoreInteractions(queryProcessor);
    }

    private long acquireConnectionId() {
        JdbcConnectResult result = await(eventHandler.connect());

        assertThat(result, notNullValue());
        assertThat(result.status(), is(STATUS_SUCCESS));
        assertThat(result.hasResults(), is(true));

        return result.connectionId();
    }
}
