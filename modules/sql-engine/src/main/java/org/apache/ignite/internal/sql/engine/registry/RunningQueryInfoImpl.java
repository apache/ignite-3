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
public class RunningQueryInfoImpl implements RunningQueryInfo {
    private final UUID queryId;
    private final String schema;
    private final String sql;
    private final Instant startTime;
    private final Integer statementNumber;
    private final @Nullable String parentId;
    private final @Nullable RunningScriptInfoTracker script;

    private volatile @Nullable String txId;
    private volatile String phase;
    private volatile @Nullable String queryType;
    private volatile @Nullable AsyncSqlCursor<?> cursor;

    /**
     * Constructor.
     */
    public RunningQueryInfoImpl(
            UUID queryId,
            String schema,
            String sql,
            Instant startTime,
            @Nullable String transactionId,
            @Nullable String queryType,
            @Nullable Integer statementNumber,
            @Nullable RunningScriptInfoTracker script
    ) {
        this.queryId = queryId;
        this.schema = schema;
        this.sql = sql;
        this.txId = transactionId;
        this.script = script;
        this.parentId = script == null ? null : script.queryId().toString();
        this.queryType = queryType;
        this.startTime = startTime;
        this.statementNumber = statementNumber;
        this.phase = QueryExecutionPhase.INITIALIZATION.name();
    }

    @Override
    public UUID queryId() {
        return queryId;
    }

    @Override
    public String schema() {
        return schema;
    }

    @Override
    public String sql() {
        return sql;
    }

    @Override
    public Instant startTime() {
        return startTime;
    }

    @Override
    public String phase() {
        return phase;
    }

    @Override
    public void phase(QueryExecutionPhase phase) {
        this.phase = phase.name();
    }

    @Override
    public @Nullable String queryType() {
        return queryType;
    }

    @Override
    public void queryType(SqlQueryType queryType) {
        this.queryType = queryType.name();
    }

    @Override
    public @Nullable String transactionId() {
        return txId;
    }

    @Override
    public void transactionId(UUID id) {
        assert txId == null;

        txId = id.toString();
    }

    @Override
    public @Nullable AsyncSqlCursor<?> cursor() {
        return cursor;
    }

    @Override
    public void cursor(AsyncSqlCursor<?> cursor) {
        Objects.requireNonNull(cursor, "cursor");

        assert this.cursor == null;

        this.cursor = cursor;
    }

    @Override
    public @Nullable String parentId() {
        return parentId;
    }

    @Override
    public @Nullable Integer statementNumber() {
        return statementNumber;
    }

    @Nullable RunningScriptInfoTracker script() {
        return script;
    }
}
