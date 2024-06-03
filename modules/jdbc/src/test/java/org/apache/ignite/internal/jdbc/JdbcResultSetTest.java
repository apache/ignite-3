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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit test for JdbcResultSet. */
@ExtendWith(MockitoExtension.class)
public class JdbcResultSetTest extends BaseIgniteAbstractTest {
    @Test
    public void getNextResultSetTest() throws SQLException {
        String errorStr = "Failed to fetch query results";

        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = spy(new JdbcResultSet(handler, stmt, 1L, 1, true, List.of(), true, 0, false, 1, null));

        when(handler.getMoreResultsAsync(any())).thenReturn(CompletableFuture.completedFuture(
                new JdbcQuerySingleResult(Response.STATUS_FAILED, errorStr)));

        JdbcQueryCloseResult closeRes = mock(JdbcQueryCloseResult.class);

        when(handler.closeAsync(any())).thenReturn(CompletableFuture.completedFuture(closeRes));
        when(closeRes.hasResults()).thenReturn(true);

        SQLException ex = assertThrows(SQLException.class, rs::getNextResultSet);

        String actualMessage = ex.getMessage();

        assertEquals(errorStr, actualMessage);
        assertEquals(codeToSqlState(Response.STATUS_FAILED), ex.getSQLState());

        verify(rs).close0(anyBoolean());
    }

    @Test
    public void checkClose() throws SQLException {
        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = spy(new JdbcResultSet(handler, stmt, 1L, 1, true, List.of(), true, 0, false, 1, null));

        JdbcQueryCloseResult closeRequest = mock(JdbcQueryCloseResult.class);

        when(closeRequest.hasResults()).thenReturn(true);

        when(handler.closeAsync(any())).thenReturn(CompletableFuture.completedFuture(closeRequest));

        rs.close();

        ArgumentCaptor<JdbcQueryCloseRequest> argument = ArgumentCaptor.forClass(JdbcQueryCloseRequest.class);

        verify(handler).closeAsync(argument.capture());

        assertFalse(argument.getValue().removeFromResources());
    }
}
