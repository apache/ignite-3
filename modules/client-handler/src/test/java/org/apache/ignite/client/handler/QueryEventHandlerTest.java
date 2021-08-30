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
 */

package org.apache.ignite.client.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQuery;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchResult;
import org.apache.ignite.client.proto.query.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.ignite.client.proto.query.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;
import static org.apache.ignite.client.proto.query.event.JdbcResponse.STATUS_FAILED;
import static org.apache.ignite.client.proto.query.event.JdbcResponse.STATUS_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Test QueryEventHandler implementation with various request types.
 * */
@ExtendWith(MockitoExtension.class)
public class QueryEventHandlerTest {
    /**
     * Mocked query processor.
     */
    @Mock
    private QueryProcessor processor;

    /**
     * Mocked sql cursor.
     */
    @Mock
    private SqlCursor<List<?>> cursor;

    /**
     * Test multiple select query request.
     */
    @Test
    public void testSelectQueryRequestMultipleStatements() {
        int cursorSize = 10;

        JdbcQueryEventHandler hnd = prepareHandlerForMultiState(cursorSize);

        JdbcQueryExecuteRequest qryReq = getJdbcQueryExecuteRequest(10);

        JdbcQueryExecuteResult res = hnd.query(qryReq);

        assertEquals(res.status(), STATUS_SUCCESS);
        assertNull(res.err());

        assertEquals(res.results().size(), cursorSize);

        for (int i = 0; i < res.results().size(); i++) {
            JdbcQuerySingleResult singleRes = res.results().get(i);
            assertTrue(singleRes.isQuery());
            assertFalse(singleRes.last());
            assertEquals(singleRes.cursorId(), i);

            assertEquals(singleRes.items().size(), 10);
            assertEquals(singleRes.items().get(0).size(), 1);

            assertEquals(singleRes.items().get(0).get(0), "42");
        }
    }

    /**
     * Prepare cursors and processor for multiple select query request.
     *
     * @param cursorSize Size of the cursors array.
     * @return Query event handler.
     */
    private JdbcQueryEventHandler prepareHandlerForMultiState(int cursorSize) {
        when(cursor.getQueryType()).thenReturn(SqlQueryType.QUERY);
        when(cursor.hasNext()).thenReturn(true);

        doReturn(Collections.singletonList("42")).when(cursor).next();

        List<SqlCursor<List<?>>> cursors = new ArrayList<>(cursorSize);

        for (int i = 0; i < cursorSize; i++)
            cursors.add(cursor);

        when(processor.query(anyString(), anyString(), any())).thenReturn(cursors);

        return new JdbcQueryEventHandlerImpl(processor, null);
    }

    /**
     * Test fetch query request.
     */
    @Test
    public void testFetchQueryRequest() {
        JdbcQueryEventHandler hnd = getHandler(SqlQueryType.QUERY, "42");

        JdbcQueryExecuteRequest qryReq = getJdbcQueryExecuteRequest(10);

        JdbcQueryExecuteResult qryRes = hnd.query(qryReq);

        var fetchReq = new JdbcQueryFetchRequest(qryRes.results().get(0).cursorId(), 10);

        JdbcQueryFetchResult fetchRes = hnd.fetch(fetchReq);

        assertEquals(fetchRes.status(), STATUS_SUCCESS);
        assertNull(fetchRes.err());

        assertEquals(fetchRes.items().size(), 10);

        assertEquals(fetchRes.items().get(0).get(0), "42");
    }

    /**
     * Test dml query request.
     */
    @Test
    public void testDMLQuery() {
        JdbcQueryEventHandler hnd = getHandler(SqlQueryType.DML, 1L);

        when(cursor.hasNext()).thenReturn(true).thenReturn(false);

        JdbcQueryExecuteRequest qryReq = getJdbcQueryExecuteRequest(10);

        JdbcQueryExecuteResult res = hnd.query(qryReq);

        assertEquals(res.status(), STATUS_SUCCESS);
        assertNull(res.err());

        assertEquals(res.results().size(), 1);

        JdbcQuerySingleResult singleRes = res.results().get(0);

        assertEquals(singleRes.updateCount(), 1L);
        assertFalse(singleRes.isQuery());
        assertTrue(singleRes.last());
    }

    /**
     * Test batch query request.
     */
    @Test
    public void testBatchQuery() {
        JdbcQueryEventHandler hnd = new JdbcQueryEventHandlerImpl(processor, null);

        var req = new JdbcBatchExecuteRequest(
            "PUBLIC",
            Collections.singletonList(new JdbcQuery("INSERT INTRO test VALUES (1);", null)),
            false);

        JdbcBatchExecuteResult batch = hnd.batch(req);

        assertEquals(batch.status(), UNSUPPORTED_OPERATION);
        assertNotNull(batch.err());
    }

    /**
     * Test error cases for select query request.
     */
    @Test
    public void testSelectQueryBadRequest() {
        JdbcQueryEventHandler hnd = new JdbcQueryEventHandlerImpl(processor, null);

        JdbcQueryExecuteRequest qryReq = getJdbcQueryExecuteRequest(10);

        JdbcQueryExecuteResult res1 = hnd.query(qryReq);

        assertEquals(res1.status(), STATUS_FAILED);
        assertNotNull(res1.err());

        JdbcQueryExecuteRequest req2 = getJdbcQueryExecuteRequest(10);

        when(processor.query(anyString(), anyString(), any())).thenReturn(Collections.emptyList());

        JdbcQueryExecuteResult res2 = hnd.query(req2);

        assertEquals(res2.status(), STATUS_FAILED);
        assertNotNull(res2.err());

        when(cursor.hasNext()).thenReturn(true);
        when(cursor.next()).thenThrow(RuntimeException.class);
        when(processor.query(anyString(), anyString(), any())).thenReturn(Collections.singletonList(cursor));

        JdbcQueryExecuteResult res3 = hnd.query(req2);

        assertEquals(res3.status(), STATUS_FAILED);
        assertNotNull(res3.err());
    }

    /**
     * Test error cases for fetch query request.
     */
    @Test
    public void testFetchQueryBadRequests() {
        JdbcQueryEventHandler hnd = getHandler(SqlQueryType.QUERY, "42");

        JdbcQueryExecuteRequest qryReq = getJdbcQueryExecuteRequest(1);

        JdbcQueryExecuteResult qryRes = hnd.query(qryReq);

        var fetchReq = new JdbcQueryFetchRequest(qryRes.results().get(0).cursorId(), -1);

        JdbcQueryFetchResult fetchRes = hnd.fetch(fetchReq);

        assertEquals(fetchRes.status(), STATUS_FAILED);
        assertNotNull(fetchRes.err());

        fetchReq = new JdbcQueryFetchRequest(Integer.MAX_VALUE, 1);

        fetchRes = hnd.fetch(fetchReq);

        assertEquals(fetchRes.status(), STATUS_FAILED);
        assertNotNull(fetchRes.err());
    }

    /**
     * Test close cursor request.
     */
    @Test
    public void testCloseRequest() {
        JdbcQueryEventHandler hnd = getHandler(SqlQueryType.QUERY, "42");

        JdbcQueryExecuteRequest qryReq = getJdbcQueryExecuteRequest(1);

        JdbcQueryExecuteResult qryRes = hnd.query(qryReq);

        var closeReq = new JdbcQueryCloseRequest(qryRes.results().get(0).cursorId());

        JdbcQueryCloseResult closeRes = hnd.close(closeReq);

        assertEquals(closeRes.status(), STATUS_SUCCESS);

        closeRes = hnd.close(closeReq);

        assertEquals(closeRes.status(), STATUS_FAILED);
        assertNotNull(closeRes.err());
    }

    /**
     * Prepare getJdbcQueryExecuteRequest.
     *
     * @param pageSize Size of result set in response.
     * @return JdbcQueryExecuteRequest.
     */
    private JdbcQueryExecuteRequest getJdbcQueryExecuteRequest(int pageSize) {
        return new JdbcQueryExecuteRequest("PUBLIC", pageSize, 3, false,
            false, "SELECT * FROM Test;", null);
    }

    /**
     * Prepare cursor and processor for multiple select query request.
     *
     * @param type Expected sql query type.
     * @param val Value in result set.
     * @return Query event handler.
     */
    private JdbcQueryEventHandler getHandler(SqlQueryType type, Object val) {
        when(cursor.getQueryType()).thenReturn(type);
        when(cursor.hasNext()).thenReturn(true);

        doReturn(Collections.singletonList(val)).when(cursor).next();

        when(processor.query(anyString(), anyString(), any())).thenReturn(Collections.singletonList(cursor));

        return new JdbcQueryEventHandlerImpl(processor, null);
    }
}
