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

package org.apache.ignite.internal.jdbc;

import static org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode.codeToSqlState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit test for JdbcResultSet. */
@ExtendWith(MockitoExtension.class)
public class JdbcResultSetTest extends BaseIgniteAbstractTest {
    @Test
    public void exceptionIsPropagatedFromGetNextResultResponse() throws SQLException {
        String errorStr = "Failed to fetch query results";

        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = createResultSet(handler, stmt, true);

        when(handler.getMoreResultsAsync(any())).thenReturn(CompletableFuture.completedFuture(
                new JdbcQuerySingleResult(Response.STATUS_FAILED, errorStr)));

        SQLException ex = assertThrows(SQLException.class, rs::getNextResultSet);

        String actualMessage = ex.getMessage();

        assertEquals(errorStr, actualMessage);
        assertEquals(codeToSqlState(Response.STATUS_FAILED), ex.getSQLState());

        verify(handler).getMoreResultsAsync(any());
        verifyNoMoreInteractions(handler);

        assertTrue(rs.isClosed());
    }

    @Test
    public void getNextResultWhenNextResultAvailable() throws SQLException {
        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = createResultSet(handler, stmt, true);

        int expectedUpdateCount = 10;

        when(handler.getMoreResultsAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(new JdbcQuerySingleResult(null, expectedUpdateCount, false)));

        JdbcResultSet nextRs = rs.getNextResultSet();

        assertNotNull(nextRs);
        assertEquals(expectedUpdateCount, nextRs.updatedCount());

        verify(handler).getMoreResultsAsync(any());
        verifyNoMoreInteractions(handler);

        assertTrue(rs.isClosed());
    }

    @Test
    public void getNextResultWhenNextResultIsNotAvailable() throws SQLException {
        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = createResultSet(handler, stmt, false);

        when(handler.closeAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(new JdbcQueryCloseResult()));

        JdbcResultSet nextRs = rs.getNextResultSet();

        assertNull(nextRs);

        verify(handler).closeAsync(any());
        verifyNoMoreInteractions(handler);

        assertTrue(rs.isClosed());
    }

    @ParameterizedTest(name = "hasNextResult: {0}")
    @ValueSource(booleans = {true, false})
    public void checkClose(boolean hasNextResult) throws SQLException {
        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = createResultSet(handler, stmt, hasNextResult);

        when(handler.closeAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(new JdbcQueryCloseResult()));

        rs.close();

        ArgumentCaptor<JdbcQueryCloseRequest> argument = ArgumentCaptor.forClass(JdbcQueryCloseRequest.class);

        verify(handler).closeAsync(argument.capture());

        assertEquals(!hasNextResult, argument.getValue().removeFromResources());
    }

    private static JdbcResultSet createResultSet(
            JdbcQueryCursorHandler handler,
            JdbcStatement statement,
            boolean hasNextResult
    ) {
        return new JdbcResultSet(handler, statement, 1L, 1, true, List.of(), true, hasNextResult, 0, false, 1, r -> List.of());
    }
}
