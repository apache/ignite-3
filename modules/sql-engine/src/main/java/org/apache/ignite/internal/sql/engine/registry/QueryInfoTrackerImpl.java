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

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.jetbrains.annotations.Nullable;

class QueryInfoTrackerImpl implements QueryInfoTracker {
    private final UUID queryId;
    private final ScriptInfoTracker script;
    private final Map<UUID, RunningQueryInfo> runningQueries;
    private final AtomicInteger openedCursorsCount;
    private final RunningQueryInfo queryInfo;

    QueryInfoTrackerImpl(
            UUID queryId,
            RunningQueryInfo queryInfo,
            Map<UUID, RunningQueryInfo> runningQueries,
            @Nullable ScriptInfoTracker script,
            AtomicInteger openedCursorsCount
    ) {
        this.queryId = queryId;
        this.queryInfo = queryInfo;
        this.runningQueries = runningQueries;
        this.openedCursorsCount = openedCursorsCount;
        this.script = script;
    }

    @Override
    public UUID queryId() {
        return queryId;
    }

    @Override
    public void changePhase(QueryExecutionPhase phase) {
        Objects.requireNonNull(phase, "phase");

        queryInfo.setPhase(phase);
    }

    @Override
    public void changeType(SqlQueryType queryType) {
        Objects.requireNonNull(queryType, "queryType");

        queryInfo.setType(queryType);
    }

    @Override
    public void changeTransactionId(UUID txId) {
        Objects.requireNonNull(txId, "txId");

        queryInfo.setTransactionId(txId);
    }

    @Override
    public void setCursor(AsyncSqlCursor<?> cursor) {
        openedCursorsCount.incrementAndGet();

        queryInfo.setCursor(cursor);
    }

    @Override
    public boolean unregister() {
        RunningQueryInfo queryInfo = runningQueries.remove(queryId);

        if (queryInfo == null) {
            return false;
        }

        if (queryInfo.cursor() != null) {
            openedCursorsCount.decrementAndGet();
        }

        if (script != null) {
            script.tryUnregister();
        }

        return true;
    }
}
