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

package org.apache.ignite.internal.client.query;

import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchResult;
import org.apache.ignite.internal.client.TcpIgniteClient;

/**
 *  Jdbc query network event handler implementation.
 */
public class JdbcClientQueryEventHandler implements JdbcQueryEventHandler {
    /** Channel. */
    private final TcpIgniteClient client;

    /**
     * @param client TcpIgniteClient.
     */
    public JdbcClientQueryEventHandler(TcpIgniteClient client) {
        this.client = client;
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryExecuteResult query(JdbcQueryExecuteRequest req) {
        JdbcQueryExecuteResult res = new JdbcQueryExecuteResult();

        client.sendRequest(ClientOp.SQL_EXEC, req, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryFetchResult fetch(JdbcQueryFetchRequest req) {
        JdbcQueryFetchResult res = new JdbcQueryFetchResult();

        client.sendRequest(ClientOp.SQL_NEXT, req, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public JdbcBatchExecuteResult batch(JdbcBatchExecuteRequest req) {
        JdbcBatchExecuteResult res = new JdbcBatchExecuteResult();

        client.sendRequest(ClientOp.SQL_EXEC_BATCH, req, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryCloseResult close(JdbcQueryCloseRequest req) {
        JdbcQueryCloseResult res = new JdbcQueryCloseResult();

        client.sendRequest(ClientOp.SQL_CURSOR_CLOSE, req, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public JdbcMetaTablesResult tablesMeta(JdbcMetaTablesRequest req) {
        JdbcMetaTablesResult res = new JdbcMetaTablesResult();

        client.sendRequest(ClientOp.SQL_TABLE_META, req, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public JdbcMetaColumnsResult columnsMeta(JdbcMetaColumnsRequest req) {
        JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

        client.sendRequest(ClientOp.SQL_COLUMN_META, req, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public JdbcMetaSchemasResult schemasMeta(JdbcMetaSchemasRequest req) {
        JdbcMetaSchemasResult res = new JdbcMetaSchemasResult();

        client.sendRequest(ClientOp.SQL_SCHEMAS_META, req, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public JdbcMetaPrimaryKeysResult primaryKeysMeta(JdbcMetaPrimaryKeysRequest req) {
        JdbcMetaPrimaryKeysResult res = new JdbcMetaPrimaryKeysResult();

        client.sendRequest(ClientOp.SQL_PK_META, req, res);

        return res;
    }
}
