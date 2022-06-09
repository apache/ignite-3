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
import org.apache.ignite.internal.jdbc.proto.event.BatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.BatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.BatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.ClientMessageUtils;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesResult;
import org.apache.ignite.internal.jdbc.proto.event.QueryExecuteRequest;

/**
 * Jdbc query network event handler implementation.
 */
public class JdbcClientQueryEventHandler {
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
    public CompletableFuture<ClientQueryExecuteResult> queryAsync(QueryExecuteRequest req) {
        return client.sendRequestAsync(ClientOp.JDBC_EXEC,  w -> req.writeBinary(w.out()), p -> {
            ClientQueryExecuteResult res = new ClientQueryExecuteResult(p.clientChannel());
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    public CompletableFuture<BatchExecuteResult> batchAsync(BatchExecuteRequest req) {
        BatchExecuteResult res = new BatchExecuteResult();

        return client.sendRequestAsync(ClientOp.JDBC_EXEC_BATCH, req, res);
    }

    /** {@inheritDoc} */
    public CompletableFuture<BatchExecuteResult> batchPrepStatementAsync(
            BatchPreparedStmntRequest req) {
        BatchExecuteResult res = new BatchExecuteResult();

        return client.sendRequestAsync(ClientOp.JDBC_SQL_EXEC_PS_BATCH, req, res);
    }

    /** {@inheritDoc} */
    public CompletableFuture<JdbcMetaTablesResult> tablesMetaAsync(JdbcMetaTablesRequest req) {
        JdbcMetaTablesResult res = new JdbcMetaTablesResult();

        return client.sendRequestAsync(ClientOp.JDBC_TABLE_META, req, res);
    }

    /** {@inheritDoc} */
    public CompletableFuture<JdbcMetaColumnsResult> columnsMetaAsync(String schemaName, String tblName, String colName) {
        JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

        return client.sendRequestAsync(ClientOp.JDBC_COLUMN_META, w -> {
            ClientMessageUtils.writeStringNullable(w.out(), schemaName);
            ClientMessageUtils.writeStringNullable(w.out(), tblName);
            ClientMessageUtils.writeStringNullable(w.out(), colName);
        }, r -> {
            res.readBinary(r.in());

            return res;
        });
    }

    /** {@inheritDoc} */
    public CompletableFuture<JdbcMetaSchemasResult> schemasMetaAsync(String schemaName) {
        JdbcMetaSchemasResult res = new JdbcMetaSchemasResult();

        return client.sendRequestAsync(ClientOp.JDBC_SCHEMAS_META, w -> {
            ClientMessageUtils.writeStringNullable(w.out(), schemaName);
        }, r -> {
            res.readBinary(r.in());

            return res;
        });
    }

    /** {@inheritDoc} */
    public CompletableFuture<JdbcMetaPrimaryKeysResult> primaryKeysMetaAsync(String schemaName, String tblName) {
        JdbcMetaPrimaryKeysResult res = new JdbcMetaPrimaryKeysResult();

        return client.sendRequestAsync(ClientOp.JDBC_PK_META, w -> {
            ClientMessageUtils.writeStringNullable(w.out(), schemaName);
            ClientMessageUtils.writeStringNullable(w.out(), tblName);
        }, r -> {
            res.readBinary(r.in());

            return res;
        });
    }
}
