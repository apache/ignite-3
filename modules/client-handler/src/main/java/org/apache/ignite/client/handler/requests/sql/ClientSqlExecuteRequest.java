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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.IgniteSql;
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
        Statement statement = readStatement(in, sql);
        Object[] arguments = readArguments(in);

        return sql.createSession()
                .executeAsync(tx, statement, arguments)
                .thenCompose(asyncResultSet -> writeResultSetAsync(out, resources, asyncResultSet));
    }

    private static CompletionStage<Void> writeResultSetAsync(
            ClientMessagePacker out,
            ClientResourceRegistry resources,
            AsyncResultSet asyncResultSet) {
        if (asyncResultSet.hasRowSet() && asyncResultSet.hasMorePages()) {
            try {
                ClientResource resource = new ClientResource(
                        asyncResultSet,
                        () -> asyncResultSet.closeAsync().toCompletableFuture().join());

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

        // Pack metadata.
        if (asyncResultSet.metadata() == null || asyncResultSet.metadata().columns() == null) {
            out.packArrayHeader(0);
        } else {
            List<ColumnMetadata> cols = asyncResultSet.metadata().columns();
            out.packArrayHeader(cols.size());

            for (int i = 0; i < cols.size(); i++) {
                ColumnMetadata col = cols.get(i);
                out.packString(col.name());
                out.packBoolean(col.nullable());

                // TODO: IGNITE-17052 Implement query metadata.
                // Ideally we only need the type code here.
                out.packString(col.valueClass().getName());
                out.packObjectWithType(col.type());
            }
        }

        // Pack first page.
        if (asyncResultSet.hasRowSet()) {
            packCurrentPage(out, asyncResultSet);
        } else {
            return asyncResultSet.closeAsync();
        }

        return CompletableFuture.completedFuture(null);
    }

    private static Statement readStatement(ClientMessageUnpacker in, IgniteSql sql) {
        StatementBuilder statementBuilder = sql.statementBuilder();

        if (!in.tryUnpackNil()) {
            statementBuilder.defaultSchema(in.unpackString());
        }
        if (!in.tryUnpackNil()) {
            statementBuilder.pageSize(in.unpackInt());
        }

        statementBuilder.query(in.unpackString());

        if (!in.tryUnpackNil()) {
            statementBuilder.queryTimeout(in.unpackLong(), TimeUnit.MILLISECONDS);
        }

        statementBuilder.prepared(in.unpackBoolean());

        var propCount = in.unpackMapHeader();

        for (int i = 0; i < propCount; i++) {
            statementBuilder.property(in.unpackString(), in.unpackObjectWithType());
        }

        return statementBuilder.build();
    }

    private static Object[] readArguments(ClientMessageUnpacker in) {
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
}
