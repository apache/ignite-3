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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.lang.ErrorGroups.Sql.SESSION_CLOSED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.IgniteTransactions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests to verify {@link SessionImpl}.
 */
@SuppressWarnings({"resource", "ThrowableNotThrown"})
@ExtendWith(MockitoExtension.class)
class SessionImplTest extends BaseIgniteAbstractTest {
    private final ConcurrentMap<SessionId, SessionImpl> sessions = new ConcurrentHashMap<>();

    private final AtomicLong clock = new AtomicLong();

    @Mock
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        clock.set(1L);

        when(queryProcessor.closeSession(any()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @AfterEach
    void cleanUp() {
        sessions.values().forEach(SessionImpl::close);

        sessions.clear();
    }

    @Test
    void sessionExpiredAfterIdleTimeout() {
        SessionImpl session = newSession(2);
        assertThat(session.expired(), is(false));

        //period is small to expire session
        clock.addAndGet(1);
        assertThat(session.expired(), is(false));

        //period is enough to session expired
        clock.addAndGet(2);
        assertThat(session.expired(), is(true));
    }

    @Test
    void sessionCleanUpsItselfFromSessionsMapOnClose() {
        SessionImpl session = newSession(3);
        assertThat(sessions.get(session.id()), sameInstance(session));

        session.close();

        assertThat(sessions.get(session.id()), nullValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    void sessionsShouldNotExpireWhenNewQueryExecuted() {
        AsyncSqlCursor<List<Object>> result = mock(AsyncSqlCursor.class);

        when(result.requestNextAsync(anyInt()))
                .thenReturn(CompletableFuture.completedFuture(new BatchedResult<>(List.of(List.of(0L)), false)));

        when(queryProcessor.querySingleAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        SessionImpl session = newSession(3);

        clock.addAndGet(2);

        await(session.executeAsync(null, "SELECT 1", 1));

        clock.addAndGet(2);
        assertThat(session.expired(), is(false));

        await(session.executeBatchAsync(null, "SELECT 1", BatchedArguments.of()));

        clock.addAndGet(2);
        assertThat(session.expired(), is(false));

        clock.addAndGet(2);

        assertThrowsSqlException(
                SESSION_CLOSED_ERR,
                "Session is closed",
                () -> await(session.executeAsync(null, "SELECT 1"))
        );

        assertThrowsSqlException(
                SESSION_CLOSED_ERR,
                "Session is closed",
                () -> await(session.executeBatchAsync(null, "SELECT 1", BatchedArguments.of()))
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void sessionsShouldNotExpireWhenResultSetAreProcessed() {
        AsyncSqlCursor<List<Object>> result = mock(AsyncSqlCursor.class);

        when(result.requestNextAsync(anyInt()))
                .thenReturn(CompletableFuture.completedFuture(new BatchedResult<>(List.of(List.of(0L)), false)));
        when(result.queryType())
                .thenReturn(SqlQueryType.QUERY);

        when(queryProcessor.querySingleAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        SessionImpl session = newSession(3);

        AsyncResultSet<?> rs = await(session.executeAsync(null, "SELECT 1", 1));

        assertThat(rs, notNullValue());

        clock.addAndGet(2);
        assertThat(session.expired(), is(false));

        {
            rs.currentPage();

            clock.addAndGet(2);
            assertThat(session.expired(), is(false));
        }

        {
            rs.currentPage();

            clock.addAndGet(2);
            assertThat(session.expired(), is(false));
        }

        {
            rs.fetchNextPage();

            clock.addAndGet(2);
            assertThat(session.expired(), is(false));
        }

        {
            rs.fetchNextPage();

            clock.addAndGet(2);
            assertThat(session.expired(), is(false));
        }
    }

    private SessionImpl newSession(long idleTimeout) {
        when(queryProcessor.createSession(any()))
                .thenAnswer(ignored -> new SessionId(UUID.randomUUID()));

        SessionBuilder builder = new SessionBuilderImpl(
                new IgniteSpinBusyLock(),
                sessions,
                queryProcessor,
                mock(IgniteTransactions.class),
                clock::get,
                new HashMap<>()
        );

        return (SessionImpl) builder
                .idleTimeout(idleTimeout, TimeUnit.MILLISECONDS)
                .build();
    }
}
