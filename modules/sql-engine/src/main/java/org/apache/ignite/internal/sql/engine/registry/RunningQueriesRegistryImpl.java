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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
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

    @Override
    public QueryInfoTracker register(String schema, String sql, @Nullable UUID txId) {
        return register0(schema, sql, STATEMENT_NUM_NOT_APPLICABLE, null, toStringSafe(txId), null);
    }

    @Override
    public QueryInfoTracker registerScript(String schema, String sql, @Nullable UUID txId) {
        return register0(schema, sql, STATEMENT_NUM_NOT_APPLICABLE, "SCRIPT", toStringSafe(txId), null);
    }

    private QueryInfoTracker register0(
            String schema,
            String sql,
            int statementNum,
            @Nullable String type,
            @Nullable String txId,
            @Nullable QueryInfoTracker parent
    ) {
        UUID queryId = UUID.randomUUID();

        RunningQueryInfo queryInfo = new RunningQueryInfo(
                queryId.toString(),
                schema,
                sql,
                txId,
                toStringSafe(parent == null ? null : parent.queryId()),
                type,
                statementNum,
                Instant.now(),
                QueryExecutionPhase.INITIALIZATION.name()
        );

        runningQueries.put(queryId, queryInfo);

        return new QueryInfoTrackerImpl(queryId, parent);
    }

    private boolean doUpdate(UUID queryId, Function<RunningQueryInfo, RunningQueryInfo> replace) {
        RunningQueryInfo prevVal = runningQueries.computeIfPresent(queryId, (id, info) -> replace.apply(info));

        return prevVal != null;
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

    class QueryInfoTrackerImpl implements QueryInfoTracker {
        private final UUID queryId;
        private final QueryInfoTracker parent;

        private final AtomicInteger statementsNumber = new AtomicInteger();

        // TODO do we need volatile?
        private final AtomicInteger statementsCounter = new AtomicInteger();

        QueryInfoTrackerImpl(UUID queryId, @Nullable QueryInfoTracker parent) {
            this.queryId = queryId;
            this.parent = parent;
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
        public boolean changeType(SqlQueryType queryType) {
            Objects.requireNonNull(queryType, "queryType");

            return doUpdate(queryId, info -> info.withQueryType(queryType));
        }

        @Override
        public boolean changeTransactionId(UUID txId) {
            Objects.requireNonNull(txId, "txId");

            return doUpdate(queryId, info -> info.withTransactionId(txId));
        }

        @Override
        public boolean unregister() {
            if (parent == null) {
                int remainingCount = statementsNumber.decrementAndGet();

                return statementsCounter.get() == 0 && remainingCount <= 0
                        && runningQueries.remove(queryId) != null;
            }

            boolean unregistered = runningQueries.remove(queryId) != null;

            if (unregistered) {
                parent.unregister();
            }

            return unregistered;
        }

        @Override
        public QueryInfoTracker registerStatement(String sql, SqlQueryType queryType) {
            RunningQueryInfo info = runningQueries.get(queryId);

            statementsCounter.decrementAndGet();

            if (queryType == SqlQueryType.TX_CONTROL) {
                // TODO NOOP tracker
                return null;
            }

            return register0(info.schema(), sql, statementsNumber.incrementAndGet(), queryType.name(), info.transactionId(), this);
        }

        @Override
        public void setStatementCount(int count) {
            statementsCounter.set(count);
        }
    }
}
