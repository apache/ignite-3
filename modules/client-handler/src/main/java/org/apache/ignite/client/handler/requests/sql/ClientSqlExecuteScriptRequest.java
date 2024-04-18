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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.util.ArrayUtils;

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
            QueryProcessor sql,
            IgniteTransactionsImpl transactions
    ) {
        ClientSqlProperties props = new ClientSqlProperties(in);
        String script = in.unpackString();
        Object[] arguments = in.unpackObjectArrayFromBinaryTuple();

        if (arguments == null) {
            // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
            arguments = ArrayUtils.OBJECT_EMPTY_ARRAY;
        }

        HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());
        transactions.updateObservableTimestamp(clientTs);

        return IgniteSqlImpl.executeScriptCore(sql, transactions, () -> true, () -> {}, script, arguments, props.toSqlProps());
    }
}
