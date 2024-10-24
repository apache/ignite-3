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

package org.apache.ignite.internal.sql.engine.framework;

import java.time.Instant;
import java.util.UUID;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.registry.QueryExecutionPhase;
import org.apache.ignite.internal.sql.engine.registry.RunningQueryInfo;
import org.jetbrains.annotations.Nullable;

/**
 * No-op implementation of {@link RunningQueryInfo}.
 */
public class NoopRunningQueryInfo implements RunningQueryInfo {
    private final UUID queryId;
    private final Instant startTime;

    /**
     * Constructor.
     */
    NoopRunningQueryInfo(UUID queryId) {
        this.queryId = queryId;
        this.startTime = Instant.now();
    }

    @Override
    public UUID queryId() {
        return queryId;
    }

    @Override
    public String schema() {
        return SqlCommon.DEFAULT_SCHEMA_NAME;
    }

    @Override
    public String sql() {
        return "";
    }

    @Override
    public Instant startTime() {
        return startTime;
    }

    @Override
    public String phase() {
        return QueryExecutionPhase.INITIALIZATION.name();
    }

    @Override
    public void phase(QueryExecutionPhase phase) {
        // No-op.
    }

    @Override
    public @Nullable String queryType() {
        return null;
    }

    @Override
    public void queryType(SqlQueryType queryType) {
        // No-op.
    }

    @Override
    public @Nullable String transactionId() {
        return null;
    }

    @Override
    public void transactionId(UUID id) {
        // No-op.
    }

    @Override
    public @Nullable AsyncSqlCursor<?> cursor() {
        return null;
    }

    @Override
    public void cursor(AsyncSqlCursor<?> cursor) {
        // No-op.
    }

    @Override
    public @Nullable String parentId() {
        return null;
    }

    @Override
    public @Nullable Integer statementNumber() {
        return null;
    }
}
