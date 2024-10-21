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

package org.apache.ignite.internal.sql.engine.registry;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.jetbrains.annotations.Nullable;

/**
 * Information about a running query.
 */
public class RunningQueryInfo {
    static final int STATEMENT_NUM_NOT_APPLICABLE = -1;

    private final String queryId;
    private final String schema;
    private final String sql;
    private final Instant startTime;
    private final int statementNum;
    private final @Nullable String parentId;

    private volatile @Nullable String txId;
    private volatile String phase;
    private volatile @Nullable String queryType;
    private volatile @Nullable AsyncSqlCursor<?> cursor;

    RunningQueryInfo(
            String queryId,
            String schema,
            String sql,
            @Nullable String txId,
            @Nullable String parentId,
            @Nullable String queryType,
            int statementNum,
            Instant startTime,
            String phase,
            @Nullable AsyncSqlCursor<?> cursor
    ) {
        this.queryId = queryId;
        this.schema = schema;
        this.sql = sql;
        this.txId = txId;
        this.parentId = parentId;
        this.queryType = queryType;
        this.startTime = startTime;
        this.phase = phase;
        this.statementNum = statementNum;
        this.cursor = cursor;
    }

    void setType(SqlQueryType queryType) {
        this.queryType = queryType.name();
    }

    void setPhase(QueryExecutionPhase phase) {
        this.phase = phase.name();
    }

    void setTransactionId(UUID txId) {
        this.txId = txId.toString();
    }

    void setCursor(AsyncSqlCursor<?> cursor) {
        Objects.requireNonNull(cursor, "cursor");

        assert this.cursor == null;

        this.cursor = cursor;
    }

    public @Nullable AsyncSqlCursor<?> cursor() {
        return cursor;
    }

    String queryId() {
        return queryId;
    }

    String schema() {
        return schema;
    }

    String sql() {
        return sql;
    }

    @Nullable String transactionId() {
        return txId;
    }

    @Nullable String parentId() {
        return parentId;
    }

    Instant startTime() {
        return startTime;
    }

    String phase() {
        return phase;
    }

    @Nullable String queryType() {
        return queryType;
    }

    @Nullable Integer statementNum() {
        return statementNum == STATEMENT_NUM_NOT_APPLICABLE ? null : statementNum;
    }
}
