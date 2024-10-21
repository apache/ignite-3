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

import static org.apache.ignite.internal.sql.engine.registry.RunningQueryInfo.STATEMENT_NUM_NOT_APPLICABLE;
import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link RunningQueriesRegistry}.
 */
public class RunningQueriesRegistryImpl implements RunningQueriesRegistry {
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
    public QueryInfoTracker register(String schema, String sql, @Nullable UUID txId) {
        return register0(schema, sql, STATEMENT_NUM_NOT_APPLICABLE, null, toStringSafe(txId), null);
    }

    @Override
    public ScriptInfoTracker registerScript(String schema, String sql, @Nullable UUID txId) {
        UUID queryId = UUID.randomUUID();

        RunningQueryInfo queryInfo = registerQueryInfo(queryId, schema, sql, STATEMENT_NUM_NOT_APPLICABLE, "SCRIPT", toStringSafe(txId),
                null);

        return new ScriptInfoTrackerImpl(queryId, queryInfo, runningQueries, openedCursorsCount);
    }

    private QueryInfoTracker register0(
            String schema,
            String sql,
            int statementNum,
            @Nullable String type,
            @Nullable String txId,
            @Nullable ScriptInfoTracker parent
    ) {
        UUID queryId = UUID.randomUUID();

        RunningQueryInfo queryInfo = registerQueryInfo(queryId, schema, sql, statementNum, type, txId,
                parent == null ? null : parent.queryId());

        return new QueryInfoTrackerImpl(queryId, queryInfo, runningQueries, parent, openedCursorsCount);
    }

    private RunningQueryInfo registerQueryInfo(
            UUID queryId,
            String schema,
            String sql,
            int statementNum,
            @Nullable String type,
            @Nullable String txId,
            @Nullable UUID parentQueryId
    ) {
        RunningQueryInfo queryInfo = new RunningQueryInfo(
                queryId.toString(),
                schema,
                sql,
                txId,
                toStringSafe(parentQueryId),
                type,
                statementNum,
                Instant.now(),
                QueryExecutionPhase.INITIALIZATION.name(),
                null
        );

        runningQueries.put(queryId, queryInfo);

        return queryInfo;
    }

    @Override
    public SystemView<?> systemView() {
        Iterable<RunningQueryInfo> viewData =
                () -> runningQueries.values().iterator();

        Publisher<RunningQueryInfo> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        NativeType stringType = stringOf(Short.MAX_VALUE);
        NativeType idType = stringOf(36);

        NativeType timestampType = NativeTypes.timestamp(NativeTypes.MAX_TIME_PRECISION);

        return SystemViews.<RunningQueryInfo>nodeViewBuilder()
                .name("SQL_QUERIES")
                .nodeNameColumnAlias("INITIATOR_NODE")
                .<String>addColumn("ID", idType, RunningQueryInfo::queryId)
                .<String>addColumn("PHASE", stringOf(10), RunningQueryInfo::phase)
                .<String>addColumn("TYPE", stringOf(10), RunningQueryInfo::queryType)
                .<String>addColumn("SCHEMA", stringType, RunningQueryInfo::schema)
                .<String>addColumn("SQL", stringType, RunningQueryInfo::sql)
                .<Instant>addColumn("START_TIME", timestampType, RunningQueryInfo::startTime)
                .<String>addColumn("TRANSACTION_ID", idType, RunningQueryInfo::transactionId)
                .<String>addColumn("PARENT_ID", idType, RunningQueryInfo::parentId)
                .<Integer>addColumn("STATEMENT_NUM", NativeTypes.INT32, RunningQueryInfo::statementNum)
                .dataProvider(viewDataPublisher)
                .build();
    }

    @TestOnly
    public Collection<RunningQueryInfo> runningQueries() {
        return runningQueries.values();
    }

    private static @Nullable String toStringSafe(@Nullable UUID id) {
        return id == null ? null : id.toString();
    }
}
