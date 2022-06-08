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
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.PayloadReader;
import org.apache.ignite.internal.client.PayloadWriter;
import org.apache.ignite.internal.client.ProtocolContext;
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
    private final ClientChannel channel;

    /**
     * Constructor.
     *
     * @param client TcpIgniteClient.
     */
    public JdbcClientQueryEventHandler(TcpIgniteClient client) {
        channel = client.getOrCreateChannel();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryExecuteResult> queryAsync(QueryExecuteRequest req) {
        QueryExecuteResult res = new QueryExecuteResult();

        return channel.serviceAsync(ClientOp.JDBC_EXEC, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryFetchResult> fetchAsync(QueryFetchRequest req) {
        QueryFetchResult res = new QueryFetchResult();

        return channel.serviceAsync(ClientOp.JDBC_NEXT, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BatchExecuteResult> batchAsync(BatchExecuteRequest req) {
        BatchExecuteResult res = new BatchExecuteResult();

        return channel.serviceAsync(ClientOp.JDBC_EXEC_BATCH, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BatchExecuteResult> batchPrepStatementAsync(
            BatchPreparedStmntRequest req) {
        BatchExecuteResult res = new BatchExecuteResult();

        return channel.serviceAsync(ClientOp.SQL_EXEC_PS_BATCH, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }


    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryCloseResult> closeAsync(QueryCloseRequest req) {
        QueryCloseResult res = new QueryCloseResult();

        return channel.serviceAsync(ClientOp.JDBC_CURSOR_CLOSE, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaTablesResult> tablesMetaAsync(JdbcMetaTablesRequest req) {
        JdbcMetaTablesResult res = new JdbcMetaTablesResult();

        return channel.serviceAsync(ClientOp.JDBC_TABLE_META, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> columnsMetaAsync(JdbcMetaColumnsRequest req) {
        JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

        return channel.serviceAsync(ClientOp.JDBC_COLUMN_META, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaSchemasResult> schemasMetaAsync(JdbcMetaSchemasRequest req) {
        JdbcMetaSchemasResult res = new JdbcMetaSchemasResult();

        return channel.serviceAsync(ClientOp.JDBC_SCHEMAS_META, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaPrimaryKeysResult> primaryKeysMetaAsync(JdbcMetaPrimaryKeysRequest req) {
        JdbcMetaPrimaryKeysResult res = new JdbcMetaPrimaryKeysResult();

        return channel.serviceAsync(ClientOp.JDBC_PK_META, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> queryMetadataAsync(JdbcQueryMetadataRequest req) {
        JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

        return channel.serviceAsync(ClientOp.JDBC_QUERY_META, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }
}
