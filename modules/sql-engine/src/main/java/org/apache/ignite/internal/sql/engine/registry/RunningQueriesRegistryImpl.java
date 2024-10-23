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

import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/**git
 * Implementation of {@link RunningQueriesRegistry}.
 */
public class RunningQueriesRegistryImpl implements RunningQueriesRegistry {
    public static final String SCRIPT_QUERY_TYPE = "SCRIPT";

    private final Map<UUID, RunningQueryInfo> runningQueries = new ConcurrentHashMap<>();

    private final AtomicInteger openedCursorsCount = new AtomicInteger();

    @Override
    public int openedCursorsCount() {
        return openedCursorsCount.get();
    }

    @Override
    public Collection<RunningQueryInfo> queries() {
        return runningQueries.values();
    }

    @Override
    public RunningQueryInfo registerQuery(String schema, String sql, @Nullable QueryTransactionWrapper txWrapper) {
        String explicitTxId = txWrapper != null ? txWrapper.unwrap().id().toString() : null;

        return putQueryInfo(UUID.randomUUID(), schema, sql, null, explicitTxId);
    }

    @Override
    public RunningScriptInfoTracker registerScript(String schema, String sql, ScriptTransactionContext scriptTxContext) {
        UUID queryId = UUID.randomUUID();
        QueryTransactionWrapper txWrapper = scriptTxContext.explicitTx();
        String txId = txWrapper != null ? txWrapper.unwrap().id().toString() : null;

        RunningQueryInfoImpl queryInfo = putQueryInfo(queryId, schema, sql, SCRIPT_QUERY_TYPE, txId);

        return new RunningScriptInfoTrackerImpl(queryInfo, scriptTxContext, runningQueries);
    }

    @Override
    public void registerCursor(RunningQueryInfo queryInfo, AsyncSqlCursor<?> cursor) {
        openedCursorsCount.incrementAndGet();

        queryInfo.cursor(cursor);

        cursor.onClose().whenComplete((r, e) -> unregister(queryInfo.queryId()));
    }

    @Override
    public void unregister(UUID uuid) {
        RunningQueryInfo info = runningQueries.remove(uuid);

        if (info != null) {
            if (info.cursor() != null) {
                openedCursorsCount.decrementAndGet();
            }

            RunningScriptInfoTracker script = ((RunningQueryInfoImpl) info).script();

            if (script != null) {
                script.onStatementUnregistered();
            }
        }
    }

    @Override
    public SystemView<RunningQueryInfo> asSystemView() {
        Publisher<RunningQueryInfo> viewDataPublisher = SubscriptionUtils.fromIterable(() -> queries().iterator());

        NativeType stringType = stringOf(Short.MAX_VALUE);
        NativeType idType = stringOf(36);

        NativeType timestampType = NativeTypes.timestamp(NativeTypes.MAX_TIME_PRECISION);

        return SystemViews.<RunningQueryInfo>nodeViewBuilder()
                .name("SQL_QUERIES")
                .nodeNameColumnAlias("INITIATOR_NODE")
                .<String>addColumn("ID", idType, info -> info.queryId().toString())
                .<String>addColumn("PHASE", stringOf(10), RunningQueryInfo::phase)
                .<String>addColumn("TYPE", stringOf(10), RunningQueryInfo::queryType)
                .<String>addColumn("SCHEMA", stringType, RunningQueryInfo::schema)
                .<String>addColumn("SQL", stringType, RunningQueryInfo::sql)
                .<Instant>addColumn("START_TIME", timestampType, RunningQueryInfo::startTime)
                .<String>addColumn("TRANSACTION_ID", idType, RunningQueryInfo::transactionId)
                .<String>addColumn("PARENT_ID", idType, RunningQueryInfo::parentId)
                .<Integer>addColumn("STATEMENT_NUM", NativeTypes.INT32, RunningQueryInfo::statementNumber)
                .dataProvider(viewDataPublisher)
                .build();
    }

    private RunningQueryInfoImpl putQueryInfo(
            UUID queryId,
            String schema,
            String sql,
            @Nullable String type,
            @Nullable String transactionId
    ) {
        RunningQueryInfoImpl queryInfo =
                new RunningQueryInfoImpl(queryId, schema, sql, Instant.now(), transactionId, type, null, null);

        runningQueries.put(queryId, queryInfo);

        return queryInfo;
    }
}
