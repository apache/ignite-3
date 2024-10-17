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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Registry of running queries.
 */
public class RunningQueryRegistry {
    private static final int NOT_APPLICABLE = -1;

    private final Map<UUID, RunningQueryInfo> runningQueries = new ConcurrentHashMap<>();

    UUID register(String schema, String sql, @Nullable UUID txId) {
        UUID queryId = UUID.randomUUID();

        RunningQueryInfo queryInfo = new RunningQueryInfo(
                queryId.toString(),
                schema,
                sql,
                txId == null ? null : txId.toString(),
                null,
                null,
                NOT_APPLICABLE,
                Instant.now(),
                ExecutionPhase.INITIALIZATION.name()
        );

        runningQueries.put(queryId, queryInfo);

        return queryId;
    }

    boolean unregister(UUID queryId) {
        return runningQueries.remove(queryId) != null;
    }

    // TODO extract such methods to separate class (without queryId)
    boolean updateTxId(UUID queryId, UUID txId) {
        Objects.requireNonNull(txId, "txId");

        return doUpdate(queryId, info -> info.withTransactionId(txId));
    }

    // TODO extract such methods to separate class (without queryId)
    boolean updateType(UUID queryId, SqlQueryType queryType) {
        Objects.requireNonNull(queryType, "queryType");

        return doUpdate(queryId, info -> info.withQueryType(queryType));
    }

    // TODO extract such methods to separate class (without queryId)
    boolean updatePhase(UUID queryId, ExecutionPhase phase) {
        Objects.requireNonNull(phase, "phase");

        return doUpdate(queryId, info -> info.withPhase(phase));
    }

    private boolean doUpdate(UUID queryId, Function<RunningQueryInfo, RunningQueryInfo> replace) {
        RunningQueryInfo prevVal = runningQueries.computeIfPresent(queryId, (id, info) -> replace.apply(info));

        return prevVal != null;
    }

    SystemView<?> systemView() {
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

    private static class RunningQueryInfo {
        private final String queryId;
        private final String schema;
        private final String sql;
        private final String txId;
        private final String parentId;
        private final Instant startTime;
        private final String phase;
        private final String queryType;
        private final int statementNum;

        RunningQueryInfo(
                String queryId,
                String schema,
                String sql,
                @Nullable String txId,
                @Nullable String parentId,
                @Nullable String queryType,
                int statementNum,
                Instant startTime,
                String phase
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
        }

        RunningQueryInfo withQueryType(SqlQueryType queryType) {
            return new RunningQueryInfo(queryId, schema, sql, txId, parentId,
                    queryType.name(), statementNum, startTime, phase);
        }

        RunningQueryInfo withSchema(String schema) {
            return new RunningQueryInfo(queryId, schema, sql, txId, parentId,
                    queryType, statementNum, startTime, phase);
        }

        RunningQueryInfo withPhase(ExecutionPhase phase) {
            return new RunningQueryInfo(queryId, schema, sql, txId, parentId,
                    queryType, statementNum, startTime, phase.name());
        }

        RunningQueryInfo withTransactionId(UUID txId) {
            return new RunningQueryInfo(queryId, schema, sql, txId.toString(), parentId,
                    queryType, statementNum, startTime, phase);
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
            return statementNum == NOT_APPLICABLE ? null : statementNum;
        }
    }

    enum ExecutionPhase {
        INITIALIZATION,
        OPTIMIZATION,
        EXECUTION
    }
}
