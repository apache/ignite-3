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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link RunningScriptInfoTracker}.
 */
class RunningScriptInfoTrackerImpl implements RunningScriptInfoTracker {
    private final AtomicInteger registeredStatementsCounter = new AtomicInteger();

    private final AtomicInteger remainingStatementsCounter = new AtomicInteger();

    private final Map<UUID, RunningQueryInfo> runningQueries;

    private final RunningQueryInfoImpl scriptInfo;

    private final ScriptTransactionContext scriptTxCtx;

    RunningScriptInfoTrackerImpl(
            RunningQueryInfoImpl queryInfo,
            ScriptTransactionContext scriptTxCtx,
            Map<UUID, RunningQueryInfo> runningQueries
    ) {
        this.scriptInfo = queryInfo;
        this.scriptTxCtx = scriptTxCtx;
        this.runningQueries = runningQueries;
    }

    @Override
    public UUID queryId() {
        return scriptInfo.queryId();
    }

    @Override
    public void onSubStatementUnregistered() {
        int remainingCount = registeredStatementsCounter.decrementAndGet();

        if (remainingCount == 0 && remainingStatementsCounter.get() == 0) {
            runningQueries.remove(scriptInfo.queryId());
        }
    }

    @Override
    public @Nullable RunningQueryInfo registerStatement(String sql, SqlQueryType queryType) {
        remainingStatementsCounter.decrementAndGet();

        if (queryType == SqlQueryType.TX_CONTROL) {
            // We just need to update count of processed statements.
            return null;
        }

        UUID queryId = UUID.randomUUID();

        // This can be an explicit transaction passed as a parameter
        // or started using the tx control script statement.
        QueryTransactionWrapper txWrapper = scriptTxCtx.explicitTx();

        RunningQueryInfoImpl statementQueryInfo = new RunningQueryInfoImpl(
                queryId,
                scriptInfo.schema(),
                sql,
                Instant.now(),
                txWrapper == null ? null : txWrapper.unwrap().id().toString(),
                queryType.name(),
                registeredStatementsCounter.incrementAndGet(),
                this
        );

        runningQueries.put(queryId, statementQueryInfo);

        return statementQueryInfo;
    }

    @Override
    public void onInitializationComplete(int statementsCount) {
        scriptInfo.phase(QueryExecutionPhase.EXECUTION);
        remainingStatementsCounter.set(statementsCount);
    }
}
