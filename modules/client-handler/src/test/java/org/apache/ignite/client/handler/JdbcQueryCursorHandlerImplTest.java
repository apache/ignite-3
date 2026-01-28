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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcFetchQueryResultsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.IteratorToDataCursorAdapter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test to verify {@link JdbcQueryCursorHandlerImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class JdbcQueryCursorHandlerImplTest extends BaseIgniteAbstractTest {

    @ParameterizedTest(name = "throw exception from nextResult = {0}")
    @ValueSource(booleans = {true, false})
    void testGetMoreResultsProcessExceptions(boolean nextResultThrow) throws IgniteInternalCheckedException {
        ClientResourceRegistry resourceRegistryMocked = mock(ClientResourceRegistry.class);
        ClientResource resource = mock(ClientResource.class);

        JdbcQueryCursorHandler cursorHandler = new JdbcQueryCursorHandlerImpl(resourceRegistryMocked, Runnable::run);

        when(resourceRegistryMocked.remove(anyLong())).thenReturn(resource);

        AsyncSqlCursor<InternalSqlRow> cursor = createCursor(
                nextResultThrow 
                        ? failedFuture(new SqlException(STMT_PARSE_ERR, new Exception("nextResult exception"))) 
                        : completedFuture(createCursor(null))
        );

        when(resource.get(AsyncSqlCursor.class)).thenReturn(cursor);

        JdbcQuerySingleResult result = await(
                cursorHandler.getMoreResultsAsync(new JdbcFetchQueryResultsRequest(1, 100)), 5, TimeUnit.SECONDS
        );
        assertEquals(nextResultThrow, !result.success());
    }

    @Test
    void exceptionThrownWhenCursorDoesntHaveNextResult() throws IgniteInternalCheckedException {
        ClientResourceRegistry resourceRegistryMocked = mock(ClientResourceRegistry.class);
        ClientResource resource = mock(ClientResource.class);

        JdbcQueryCursorHandler cursorHandler = new JdbcQueryCursorHandlerImpl(resourceRegistryMocked, Runnable::run);

        when(resourceRegistryMocked.remove(anyLong())).thenReturn(resource);

        AsyncSqlCursor<InternalSqlRow> cursor = createCursor(null);

        when(resource.get(AsyncSqlCursor.class)).thenReturn(cursor);

        JdbcQuerySingleResult result = await(
                cursorHandler.getMoreResultsAsync(new JdbcFetchQueryResultsRequest(1, 100)), 5, TimeUnit.SECONDS
        );
        assertFalse(result.success());
        assertThat(result.err(), containsString("Cursor doesn't have next result"));
        // handler must close cursor
        assertThat(cursor.onClose(), willCompleteSuccessfully());
    }

    private static AsyncSqlCursor<InternalSqlRow> createCursor(
            @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCursorFuture
    ) {
        ResultSetMetadata emptyMeta = new ResultSetMetadataImpl(List.of());

        return new AsyncSqlCursorImpl<>(
                SqlQueryType.QUERY,
                emptyMeta,
                new IteratorToDataCursorAdapter<>(Collections.emptyIterator()),
                nextCursorFuture
        );
    }
}
