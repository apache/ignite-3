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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Catalog.VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcFetchQueryResultsRequest;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

/**
 * Test to verify {@link JdbcQueryCursorHandlerImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class JdbcQueryCursorHandlerImplTest extends BaseIgniteAbstractTest {

    @ParameterizedTest(name = "throw exception from nextResult = {0}")
    @ValueSource(booleans = {true, false})
    void testGetMoreResultsProcessExceptions(boolean nextResultThrow) throws IgniteInternalCheckedException {
        ClientResourceRegistry resourceRegistryMocked = mock(ClientResourceRegistry.class);
        ClientResource rsrc = mock(ClientResource.class);

        JdbcQueryCursorHandler cursorHandler = new JdbcQueryCursorHandlerImpl(resourceRegistryMocked);

        when(resourceRegistryMocked.get(anyLong())).thenAnswer(new Answer<ClientResource>() {
            @Override
            public ClientResource answer(InvocationOnMock invocation) {
                return rsrc;
            }
        });

        when(rsrc.get(AsyncSqlCursor.class)).thenAnswer(new Answer<AsyncSqlCursor<InternalSqlRow>>() {
            @Override
            public AsyncSqlCursor<InternalSqlRow> answer(InvocationOnMock invocation) {
                return new AsyncSqlCursor<>() {
                    @Override
                    public SqlQueryType queryType() {
                        throw new UnsupportedOperationException("queryType");
                    }

                    @Override
                    public ResultSetMetadata metadata() {
                        throw new UnsupportedOperationException("metadata");
                    }

                    @Override
                    public boolean hasNextResult() {
                        return true;
                    }

                    @Override
                    public CompletableFuture<Void> onClose() {
                        return null;
                    }

                    @Override
                    public CompletableFuture<Void> onFirstPageReady() {
                        return null;
                    }

                    @Override
                    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextResult() {
                        if (nextResultThrow) {
                            throw new SqlException(STMT_PARSE_ERR, new Exception("nextResult exception"));
                        } else {
                            AsyncSqlCursorImpl<InternalSqlRow> sqlCursor = mock(AsyncSqlCursorImpl.class);

                            lenient().when(sqlCursor.requestNextAsync(anyInt()))
                                    .thenAnswer((Answer<BatchedResult<InternalSqlRow>>) invocation -> {
                                        throw new IgniteInternalException(VALIDATION_ERR, "requestNextAsync error");
                                    }
                            );

                            return CompletableFuture.completedFuture(sqlCursor);
                        }
                    }

                    @Override
                    public CompletableFuture<BatchedResult<InternalSqlRow>> requestNextAsync(int rows) {
                        return nullCompletedFuture();
                    }

                    @Override
                    public CompletableFuture<Void> closeAsync() {
                        return nullCompletedFuture();
                    }
                };
            }
        });

        await(cursorHandler.getMoreResultsAsync(new JdbcFetchQueryResultsRequest(1, 100)), 5, TimeUnit.SECONDS);
    }
}
