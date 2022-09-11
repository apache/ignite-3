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

package org.apache.ignite.client.handler.requests.sql;

import static org.apache.ignite.client.handler.requests.sql.ClientSqlCommon.packCurrentPage;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTx;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientSqlColumnTypeConverter;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnMetadata.ColumnOrigin;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;

/**
 * Client SQL execute request.
 */
public class ClientSqlExecuteRequest {
    /**
     * Processes the request.
     *
     * @param in  Unpacker.
     * @param out Packer.
     * @param sql SQL API.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteSql sql,
            ClientResourceRegistry resources) {
        var tx = readTx(in, resources);
        Session session = readSession(in, sql);
        Statement statement = readStatement(in, sql);
        Object[] arguments = readArguments(in);

        return session
                .executeAsync(tx, statement, arguments)
                .thenCompose(asyncResultSet -> writeResultSetAsync(out, resources, asyncResultSet, session));
    }

    private static CompletionStage<Void> writeResultSetAsync(
            ClientMessagePacker out,
            ClientResourceRegistry resources,
            AsyncResultSet asyncResultSet,
            Session session) {
        boolean hasResource = asyncResultSet.hasRowSet() && asyncResultSet.hasMorePages();

        if (hasResource) {
            try {
                var clientResultSet = new ClientSqlResultSet(asyncResultSet, session);

                ClientResource resource = new ClientResource(
                        clientResultSet,
                        () -> clientResultSet.closeAsync().join());

                out.packLong(resources.put(resource));
            } catch (IgniteInternalCheckedException e) {
                return asyncResultSet
                        .closeAsync()
                        .thenRun(() -> {
                            throw new IgniteInternalException(e.getMessage(), e);
                        });
            }
        } else {
            out.packNil(); // resourceId
        }

        out.packBoolean(asyncResultSet.hasRowSet());
        out.packBoolean(asyncResultSet.hasMorePages());
        out.packBoolean(asyncResultSet.wasApplied());
        out.packLong(asyncResultSet.affectedRows());

        packMeta(out, asyncResultSet.metadata());

        // Pack first page.
        if (asyncResultSet.hasRowSet()) {
            packCurrentPage(out, asyncResultSet);

            return hasResource
                    ? CompletableFuture.completedFuture(null)
                    : asyncResultSet.closeAsync().thenCompose(res -> session.closeAsync());
        } else {
            return asyncResultSet.closeAsync().thenCompose(res -> session.closeAsync());
        }
    }

    private static Statement readStatement(ClientMessageUnpacker in, IgniteSql sql) {
        StatementBuilder statementBuilder = sql.statementBuilder();

        statementBuilder.query(in.unpackString());

        return statementBuilder.build();
    }

    private static Session readSession(ClientMessageUnpacker in, IgniteSql sql) {
        SessionBuilder sessionBuilder = sql.sessionBuilder();

        if (!in.tryUnpackNil()) {
            sessionBuilder.defaultSchema(in.unpackString());
        }
        if (!in.tryUnpackNil()) {
            sessionBuilder.defaultPageSize(in.unpackInt());
        }

        if (!in.tryUnpackNil()) {
            sessionBuilder.defaultQueryTimeout(in.unpackLong(), TimeUnit.MILLISECONDS);
        }

        if (!in.tryUnpackNil()) {
            sessionBuilder.idleTimeout(in.unpackLong(), TimeUnit.MILLISECONDS);
        }

        var propCount = in.unpackMapHeader();

        for (int i = 0; i < propCount; i++) {
            sessionBuilder.property(in.unpackString(), in.unpackObjectWithType());
        }

        return sessionBuilder.build();
    }

    private static Object[] readArguments(ClientMessageUnpacker in) {
        if (in.tryUnpackNil()) {
            return null;
        }

        int size = in.unpackArrayHeader();

        if (size == 0) {
            return ArrayUtils.OBJECT_EMPTY_ARRAY;
        }

        var res = new Object[size];

        for (int i = 0; i < size; i++) {
            res[i] = in.unpackObjectWithType();
        }

        return res;
    }

    private static void packMeta(ClientMessagePacker out, ResultSetMetadata meta) {
        // TODO IGNITE-17179 metadata caching - avoid sending same meta over and over.
        if (meta == null || meta.columns() == null) {
            out.packArrayHeader(0);
            return;
        }

        List<ColumnMetadata> cols = meta.columns();
        out.packArrayHeader(cols.size());

        // In many cases there are multiple columns from the same table.
        // Schema is the same for all columns in most cases.
        // When table or schema name was packed before, pack index instead of string.
        Map<String, Integer> schemas = new HashMap<>();
        Map<String, Integer> tables = new HashMap<>();

        for (int i = 0; i < cols.size(); i++) {
            ColumnMetadata col = cols.get(i);

            out.packString(col.name());
            out.packBoolean(col.nullable());
            out.packInt(ClientSqlColumnTypeConverter.columnTypeToOrdinal(col.type()));
            out.packInt(col.scale());
            out.packInt(col.precision());

            ColumnOrigin origin = col.origin();

            if (origin == null) {
                out.packBoolean(false);
                continue;
            }

            out.packBoolean(true);

            if (col.name().equals(origin.columnName())) {
                out.packNil();
            } else {
                out.packString(origin.columnName());
            }

            Integer schemaIdx = schemas.get(origin.schemaName());

            if (schemaIdx == null) {
                schemas.put(origin.schemaName(), i);
                out.packString(origin.schemaName());
            } else {
                out.packInt(schemaIdx);
            }

            Integer tableIdx = tables.get(origin.tableName());

            if (tableIdx == null) {
                tables.put(origin.tableName(), i);
                out.packString(origin.tableName());
            } else {
                out.packInt(tableIdx);
            }
        }
    }
}
