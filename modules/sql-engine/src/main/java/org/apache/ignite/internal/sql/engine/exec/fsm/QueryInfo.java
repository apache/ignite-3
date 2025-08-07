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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.time.Instant;
import java.util.UUID;
import org.apache.ignite.internal.sql.SqlQueryType;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Read-only snapshot of a query state. */
public class QueryInfo {
    private final UUID id;
    private final @Nullable UUID parentId;
    private final String schema;
    private final String sql;
    private final Instant startTime;
    private final ExecutionPhase phase;
    private final @Nullable SqlQueryType queryType;
    private final @Nullable UUID transactionId;
    private final int statementNum;
    private final boolean script;
    private final @Nullable Throwable error;

    QueryInfo(Query query) {
        id = query.id;
        parentId = query.parentId;
        statementNum = query.statementNum;
        schema = query.properties.defaultSchema();
        sql = query.sql;
        startTime = query.createdAt;
        phase = query.currentPhase();
        queryType = deriveQueryType(query.parsedResult);
        transactionId = deriveTxId(query);
        error = query.error.get();

        script = query.parsedScript != null;
    }

    private static @Nullable SqlQueryType deriveQueryType(@Nullable ParsedResult parsedResult) {
        return parsedResult == null ? null : parsedResult.queryType();
    }

    private static @Nullable UUID deriveTxId(Query query) {
        QueryTransactionWrapper explicit = query.txContext.explicitTx();

        if (explicit != null) {
            return explicit.unwrap().id();
        }

        QueryTransactionWrapper tx = query.usedTransaction;

        return tx != null ? tx.unwrap().id() : null;
    }

    /** Returns {@code true} if current query is a script, returns {@code false} otherwise. */
    public boolean script() {
        return script;
    }

    /** Returns id of the query. */
    public UUID id() {
        return id;
    }

    /** Returns id of the parent query, if any. */
    public @Nullable UUID parentId() {
        return parentId;
    }

    /** Returns name of the schema that was used to resolve non qualified object names. */
    public String schema() {
        return schema;
    }

    /** Returns an original query string. */ 
    public String sql() {
        return sql;
    }

    /** Returns time at which query appears on server. */
    public Instant startTime() {
        return startTime;
    }

    /** Returns current phase of query execution. */
    public ExecutionPhase phase() {
        return phase;
    }

    /** Returns type of the query, if known. */
    public @Nullable SqlQueryType queryType() {
        return queryType;
    }

    /** Returns id of the transaction, if known. */
    public @Nullable UUID transactionId() {
        return transactionId;
    }

    /** Returns 0-based index of query within the script, if applicable, returns {@code -1} otherwise. */ 
    public int statementNum() {
        return statementNum;
    }

    /** Returns an error if one occurred during execution and caused the query to terminate. */
    public @Nullable Throwable error() {
        return error;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
