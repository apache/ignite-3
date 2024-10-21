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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.SqlQueryType;

/**
 * TODO Provides the ability to perform on-the-fly updates to running query information.
 */
class ScriptInfoTrackerImpl implements ScriptInfoTracker {
    private final UUID queryId;

    private final AtomicInteger registeredStatementsCounter = new AtomicInteger();

    private final AtomicInteger remainingStatementsCounter = new AtomicInteger();

    private final Map<UUID, RunningQueryInfo> runningQueries;

    private final AtomicInteger openedCursorsCounter;

    ScriptInfoTrackerImpl(UUID queryId, Map<UUID, RunningQueryInfo> runningQueries, AtomicInteger openedCursorsCounter) {
        this.queryId = queryId;
        this.runningQueries = runningQueries;
        this.openedCursorsCounter = openedCursorsCounter;
    }

    @Override
    public UUID queryId() {
        return queryId;
    }

    @Override
    public boolean changePhase(QueryExecutionPhase phase) {
        Objects.requireNonNull(phase, "phase");

        return doUpdate(queryId, info -> info.withPhase(phase));
    }

    @Override
    public boolean unregister() {
        return runningQueries.remove(queryId) != null;
    }

    @Override
    public boolean tryUnregister() {
        int remainingCount = registeredStatementsCounter.decrementAndGet();

        return remainingStatementsCounter.get() == 0 && remainingCount <= 0 && unregister();
    }

    @Override
    public QueryInfoTracker registerStatement(String sql, SqlQueryType queryType) {
        RunningQueryInfo info = runningQueries.get(queryId);

        remainingStatementsCounter.decrementAndGet();

        if (queryType == SqlQueryType.TX_CONTROL) {
            // TODO NOOP tracker
            return null;
        }

        UUID queryId = UUID.randomUUID();

        RunningQueryInfo queryInfo = new RunningQueryInfo(
                queryId.toString(),
                info.schema(),
                sql,
                info.transactionId(),
                this.queryId.toString(),
                queryType.name(),
                registeredStatementsCounter.incrementAndGet(),
                Instant.now(),
                QueryExecutionPhase.INITIALIZATION.name(),
                null
        );

        runningQueries.put(queryId, queryInfo);

        return new QueryInfoTrackerImpl(queryId, runningQueries, this, openedCursorsCounter);
    }

    @Override
    public void setStatementCount(int count) {
        remainingStatementsCounter.set(count);
    }

    private boolean doUpdate(UUID queryId, Function<RunningQueryInfo, RunningQueryInfo> replace) {
        RunningQueryInfo prevVal = runningQueries.computeIfPresent(queryId, (id, info) -> replace.apply(info));

        return prevVal != null;
    }
}
