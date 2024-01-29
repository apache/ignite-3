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

package org.apache.ignite.client.handler.requests.sql;

import static org.apache.ignite.client.handler.requests.sql.ClientSqlCommon.packCurrentPage;
import static org.apache.ignite.client.handler.requests.sql.ClientSqlCommon.readSession;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTx;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL execute request.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientSqlExecuteRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param sql SQL API.
     * @param resources Resources.
     * @param metrics Metrics.
     * @param transactions Transactional facade. Used to acquire last observed time to propagate to client in response.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteSql sql,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            IgniteTransactionsImpl transactions
    ) {
        var tx = readTx(in, out, resources);
        Session session = readSession(in, sql, transactions);
        Statement statement = readStatement(in, sql);
        Object[] arguments = in.unpackObjectArrayFromBinaryTuple();

        if (arguments == null) {
            // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
            arguments = ArrayUtils.OBJECT_EMPTY_ARRAY;
        }

        // TODO IGNITE-20232 Propagate observable timestamp to sql engine using internal API.
        HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());

        transactions.updateObservableTimestamp(clientTs);

        return session
                .executeAsync(tx, statement, arguments)
                .thenCompose(asyncResultSet -> {
                    out.meta(transactions.observableTimestamp());

                    return writeResultSetAsync(out, resources, asyncResultSet, session, metrics);
                });
    }

    private static CompletionStage<Void> writeResultSetAsync(
            ClientMessagePacker out,
            ClientResourceRegistry resources,
            AsyncResultSet asyncResultSet,
            Session session,
            ClientHandlerMetricSource metrics) {
        boolean hasResource = asyncResultSet.hasRowSet() && asyncResultSet.hasMorePages();

        if (hasResource) {
            try {
                metrics.cursorsActiveIncrement();

                var clientResultSet = new ClientSqlResultSet(asyncResultSet, session, metrics);

                ClientResource resource = new ClientResource(
                        clientResultSet,
                        clientResultSet::closeAsync);

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
                    ? nullCompletedFuture()
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

    private static void packMeta(ClientMessagePacker out, @Nullable ResultSetMetadata meta) {
        // TODO IGNITE-17179 metadata caching - avoid sending same meta over and over.
        if (meta == null || meta.columns() == null) {
            out.packInt(0);
            return;
        }

        ClientSqlCommon.packColumns(out, meta.columns());
    }
}
