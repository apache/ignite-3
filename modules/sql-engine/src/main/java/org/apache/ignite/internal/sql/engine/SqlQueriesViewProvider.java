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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.sql.engine.exec.fsm.ExecutionPhase;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryExecutor;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/** Provider that creates system view exposing queries running on a node. */
public class SqlQueriesViewProvider {
    public static final String SCRIPT_QUERY_TYPE = "SCRIPT";

    private final CompletableFuture<QueryExecutor> queryExecutorFuture = new CompletableFuture<>();

    /** Initializes provided with query executor used as datasource of running queries. */
    public void init(QueryExecutor queryExecutor) {
        queryExecutorFuture.complete(queryExecutor);
    }

    /** Returns system view exposing running queries. */
    public SystemView<?> get() {
        Publisher<QueryInfo> viewDataPublisher = SubscriptionUtils.fromIterable(
                queryExecutorFuture.thenApply(queryExecutor -> () -> queryExecutor.runningQueries().iterator())
        );

        NativeType stringType = stringOf(Short.MAX_VALUE);
        NativeType idType = stringOf(36);

        NativeType timestampType = NativeTypes.timestamp(NativeTypes.MAX_TIME_PRECISION);

        return SystemViews.<QueryInfo>nodeViewBuilder()
                .name("SQL_QUERIES")
                .nodeNameColumnAlias("INITIATOR_NODE")
                .<String>addColumn("QUERY_ID", idType, info -> mapId(info.id()))
                .<String>addColumn("QUERY_PHASE", stringOf(10), info -> mapPhase(info.phase()))
                .<String>addColumn("QUERY_TYPE", stringOf(10), SqlQueriesViewProvider::deriveQueryType)
                .<String>addColumn("QUERY_DEFAULT_SCHEMA", stringType, QueryInfo::schema)
                .<String>addColumn("SQL", stringType, QueryInfo::sql)
                .<Instant>addColumn("QUERY_START_TIME", timestampType, QueryInfo::startTime)
                .<String>addColumn("TRANSACTION_ID", idType, info -> mapId(info.transactionId()))
                .<String>addColumn("PARENT_QUERY_ID", idType, info -> mapId(info.parentId()))
                .<Integer>addColumn("QUERY_STATEMENT_ORDINAL", NativeTypes.INT32, info -> mapStatementNum(info.statementNum()))
                // TODO https://issues.apache.org/jira/browse/IGNITE-24589: Next columns are deprecated and should be removed.
                //  They are kept for compatibility with 3.0 version, to allow columns being found by their old names.
                .<String>addColumn("ID", idType, info -> mapId(info.id()))
                .<String>addColumn("PHASE", stringOf(10), info -> mapPhase(info.phase()))
                .<String>addColumn("TYPE", stringOf(10), SqlQueriesViewProvider::deriveQueryType)
                .<String>addColumn("SCHEMA", stringType, QueryInfo::schema)
                .<Instant>addColumn("START_TIME", timestampType, QueryInfo::startTime)
                .<String>addColumn("PARENT_ID", idType, info -> mapId(info.parentId()))
                .<Integer>addColumn("STATEMENT_NUM", NativeTypes.INT32, info -> mapStatementNum(info.statementNum()))
                // End of legacy columns list. New columns must be added below this line.
                .dataProvider(viewDataPublisher)
                .build();
    }

    private static @Nullable String mapId(@Nullable UUID id) {
        return id == null ? null : id.toString();
    }

    private static @Nullable String deriveQueryType(QueryInfo info) {
        if (info.script()) {
            return SCRIPT_QUERY_TYPE;
        }

        SqlQueryType queryType = info.queryType();

        return queryType == null ? null : queryType.toString();
    }

    private static String mapPhase(ExecutionPhase phase) {
        switch (phase) {
            case REGISTERED: // fall through
            case PARSING:
                return "INITIALIZATION";
            case OPTIMIZING:
                return "OPTIMIZATION";
            case CURSOR_INITIALIZATION: // fall through
            case SCRIPT_INITIALIZATION: // fall through
            case EXECUTING:
                return "EXECUTION";
            case TERMINATED:
                return "TERMINATED";
            default:
                throw new AssertionError("Unknown phase " + phase);
        }
    }

    private static @Nullable Integer mapStatementNum(int statementNum) {
        return statementNum >= 0 ? statementNum : null;
    }
}
