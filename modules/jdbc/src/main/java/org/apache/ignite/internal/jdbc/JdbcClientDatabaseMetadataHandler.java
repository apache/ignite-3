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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.jdbc.proto.JdbcDatabaseMetadataHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesResult;

/**
 * Jdbc database metadata requests handler implementation.
 */
public class JdbcClientDatabaseMetadataHandler implements JdbcDatabaseMetadataHandler {
    /** Channel. */
    private final TcpIgniteClient client;

    /**
     * Constructor.
     *
     * @param client TcpIgniteClient.
     */
    public JdbcClientDatabaseMetadataHandler(TcpIgniteClient client) {
        this.client = client;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaTablesResult> tablesMetaAsync(JdbcMetaTablesRequest req) {
        return client.sendRequestAsync(ClientOp.JDBC_TABLE_META, w -> req.writeBinary(w.out()), r -> {
            JdbcMetaTablesResult res = new JdbcMetaTablesResult();

            res.readBinary(r.in());

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> columnsMetaAsync(JdbcMetaColumnsRequest req) {
        return client.sendRequestAsync(ClientOp.JDBC_COLUMN_META, w -> req.writeBinary(w.out()), r -> {
            JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

            res.readBinary(r.in());

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaSchemasResult> schemasMetaAsync(JdbcMetaSchemasRequest req) {
        return client.sendRequestAsync(ClientOp.JDBC_SCHEMAS_META, w -> req.writeBinary(w.out()), r -> {
            JdbcMetaSchemasResult res = new JdbcMetaSchemasResult();

            res.readBinary(r.in());

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaPrimaryKeysResult> primaryKeysMetaAsync(JdbcMetaPrimaryKeysRequest req) {
        return client.sendRequestAsync(ClientOp.JDBC_PK_META, w -> req.writeBinary(w.out()), r -> {
            JdbcMetaPrimaryKeysResult res = new JdbcMetaPrimaryKeysResult();

            res.readBinary(r.in());

            return res;
        });
    }
}
