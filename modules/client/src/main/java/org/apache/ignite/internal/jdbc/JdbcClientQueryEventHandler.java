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

package org.apache.ignite.internal.jdbc;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.event.BatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.BatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.BatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.QueryExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.QueryFetchRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryFetchResult;

/**
 * Jdbc query network event handler implementation.
 */
public class JdbcClientQueryEventHandler implements JdbcQueryEventHandler {
    /** Channel. */
    private final TcpIgniteClient client;

    /**
     * Constructor.
     *
     * @param client TcpIgniteClient.
     */
    public JdbcClientQueryEventHandler(TcpIgniteClient client) {
        this.client = client;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryExecuteResult> queryAsync(QueryExecuteRequest req) {
        QueryExecuteResult res = new QueryExecuteResult();

        return client.sendRequestAsync(ClientOp.JDBC_EXEC, req, res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryFetchResult> fetchAsync(QueryFetchRequest req) {
        QueryFetchResult res = new QueryFetchResult();

        return client.sendRequestAsync(ClientOp.JDBC_NEXT, req, res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BatchExecuteResult> batchAsync(BatchExecuteRequest req) {
        BatchExecuteResult res = new BatchExecuteResult();

        return client.sendRequestAsync(ClientOp.JDBC_EXEC_BATCH, req, res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BatchExecuteResult> batchPrepStatementAsync(
            BatchPreparedStmntRequest req) {
        BatchExecuteResult res = new BatchExecuteResult();

        return client.sendRequestAsync(ClientOp.SQL_EXEC_PS_BATCH, req, res);
    }


    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryCloseResult> closeAsync(QueryCloseRequest req) {
        QueryCloseResult res = new QueryCloseResult();

        return client.sendRequestAsync(ClientOp.JDBC_CURSOR_CLOSE, req, res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaTablesResult> tablesMetaAsync(JdbcMetaTablesRequest req) {
        JdbcMetaTablesResult res = new JdbcMetaTablesResult();

        return client.sendRequestAsync(ClientOp.JDBC_TABLE_META, req, res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> columnsMetaAsync(JdbcMetaColumnsRequest req) {
        JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

        return client.sendRequestAsync(ClientOp.JDBC_COLUMN_META, req, res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaSchemasResult> schemasMetaAsync(JdbcMetaSchemasRequest req) {
        JdbcMetaSchemasResult res = new JdbcMetaSchemasResult();

        return client.sendRequestAsync(ClientOp.JDBC_SCHEMAS_META, req, res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaPrimaryKeysResult> primaryKeysMetaAsync(JdbcMetaPrimaryKeysRequest req) {
        JdbcMetaPrimaryKeysResult res = new JdbcMetaPrimaryKeysResult();

        return client.sendRequestAsync(ClientOp.JDBC_PK_META, req, res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> queryMetadataAsync(JdbcQueryMetadataRequest req) {
        JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

        return client.sendRequestAsync(ClientOp.JDBC_QUERY_META, req, res);
    }
}
