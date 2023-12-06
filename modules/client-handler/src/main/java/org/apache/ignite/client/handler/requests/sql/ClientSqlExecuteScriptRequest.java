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

import static org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteRequest.readSessionProperties;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;

/**
 * Client SQL execute script request.
 */
public class ClientSqlExecuteScriptRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param sql SQL API.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            IgniteSql sql,
            IgniteTransactionsImpl transactions
    ) {
        Session session = readSession(in, sql);
        String script = in.unpackString();
        Object[] arguments = in.unpackObjectArrayFromBinaryTuple();

        if (arguments == null) {
            // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
            arguments = ArrayUtils.OBJECT_EMPTY_ARRAY;
        }

        // TODO IGNITE-20232 Propagate observable timestamp to sql engine using internal API.
        HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());

        transactions.updateObservableTimestamp(clientTs);

        return session.executeScriptAsync(script, arguments);
    }

    private static Session readSession(ClientMessageUnpacker in, IgniteSql sql) {
        SessionBuilder sessionBuilder = sql.sessionBuilder();

        if (!in.tryUnpackNil()) {
            sessionBuilder.defaultSchema(in.unpackString());
        }

        if (!in.tryUnpackNil()) {
            sessionBuilder.defaultQueryTimeout(in.unpackLong(), TimeUnit.MILLISECONDS);
        }

        if (!in.tryUnpackNil()) {
            sessionBuilder.idleTimeout(in.unpackLong(), TimeUnit.MILLISECONDS);
        }

        readSessionProperties(in, sessionBuilder);

        return sessionBuilder.build();
    }
}
