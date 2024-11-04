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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.TransactionTracker;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueGetPlan;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueModifyPlan;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Executor which accepts requests for query execution and returns cursor to the result of execution.
 */
public class QueryExecutor implements LifecycleAware {
    private final Cache<String, ParsedResult> queryToParsedResultCache;
    private final ParserService parserService;
    private final Executor executor;
    private final ScheduledExecutorService scheduler;
    private final ClockService clockService;
    private final SchemaSyncService schemaSyncService;
    private final PrepareService prepareService;
    private final CatalogService catalogService;
    private final ExecutionService executionService;
    private final SqlProperties defaultProperties;
    private final TransactionTracker transactionTracker;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ConcurrentMap<UUID, Query> runningQueries = new ConcurrentHashMap<>();

    /**
     * Creates executor.
     *
     * @param cacheFactory Factory to create cache for parsed AST.
     * @param parsedResultsCacheSize Size of the cache for parsed AST.
     * @param parserService Service to parse query string.
     * @param executor Executor to submit query execution tasks.
     * @param scheduler Scheduler to use for timeout task scheduling.
     * @param clockService Current time provider.
     * @param schemaSyncService Synchronization service to wait for metadata availability.
     * @param prepareService Service to submit optimization
     * @param catalogService Catalog service.
     * @param executionService Service to submit query plans for execution.
     * @param defaultProperties Set of properties to use as defaults.
     * @param transactionTracker Tracker to track usage of transactions by query.
     */
    public QueryExecutor(
            CacheFactory cacheFactory,
            int parsedResultsCacheSize,
            ParserService parserService,
            Executor executor,
            ScheduledExecutorService scheduler,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            PrepareService prepareService,
            CatalogService catalogService,
            ExecutionService executionService,
            SqlProperties defaultProperties,
            TransactionTracker transactionTracker
    ) {
        this.queryToParsedResultCache = cacheFactory.create(parsedResultsCacheSize);
        this.parserService = parserService;
        this.executor = executor;
        this.scheduler = scheduler;
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.prepareService = prepareService;
        this.catalogService = catalogService;
        this.executionService = executionService;
        this.defaultProperties = defaultProperties;
        this.transactionTracker = transactionTracker;
    }

    /**
     * Executes the given query with provided parameters.
     *
     * <p>This is a common entry point for both single statement and script execution.
     *
     * @param properties User query properties. See {@link QueryProperty} for available properties.
     * @param txContext Transactional context to use.
     * @param sql Query string.
     * @param params Query parameters.
     * @return Future which will be completed with cursor.
     */
    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeQuery(
            SqlProperties properties,
            QueryTransactionContext txContext,
            String sql,
            Object... params
    ) {
        SqlProperties properties0 = SqlPropertiesHelper.chain(properties, defaultProperties);

        Query query = new Query(
                Instant.ofEpochMilli(clockService.now().getPhysical()),
                this,
                UUID.randomUUID(),
                sql,
                properties0,
                txContext,
                params,
                null
        );

        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            trackQuery(query);
        } finally {
            busyLock.leaveBusy();
        }

        long queryTimeout = properties.getOrDefault(QueryProperty.QUERY_TIMEOUT, 0L);

        if (queryTimeout > 0) {
            query.cancel.setTimeout(scheduler, queryTimeout);
        }

        // next state after CURSOR_INITIALIZATION. Cursor must be ready at this point.
        CompletableFuture<Void> trigger = query.onPhaseStarted(ExecutionPhase.EXECUTING);

        query.run();

        return trigger
                .thenApply(ignored -> query.cursor);
    }

    CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeChildQuery(
            Query parent,
            QueryTransactionContext scriptTxContext,
            int statementNum,
            ParsedResult parsedQuery,
            Object[] params,
            @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCursorFuture
    ) {
        Query query = new Query(
                Instant.ofEpochMilli(clockService.now().getPhysical()),
                parent,
                parsedQuery,
                statementNum,
                UUID.randomUUID(),
                scriptTxContext,
                params,
                nextCursorFuture
        );

        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            trackQuery(query);
        } finally {
            busyLock.leaveBusy();
        }

        try {
            parent.cancel.attach(query.cancel);
        } catch (QueryCancelledException ex) {
            return CompletableFuture.failedFuture(ex);
        }

        // next state after CURSOR_INITIALIZATION. Cursor must be ready at this point.
        CompletableFuture<Void> trigger = query.onPhaseStarted(ExecutionPhase.EXECUTING);

        // Child query already has parsed AST, therefore it's safe to transfer it directly to OPTIMIZATION phase.
        query.moveTo(ExecutionPhase.OPTIMIZING);

        query.run();

        return trigger
                .thenApply(ignored -> query.cursor);
    }

    /** Looks up parsed result in cache by given query string. */
    public @Nullable ParsedResult lookupParsedResultInCache(String sql) {
        return queryToParsedResultCache.get(sql);
    }

    /** Stores the given parsed statements in cache. */
    public void updateParsedResultCache(String sql, ParsedResult result) {
        queryToParsedResultCache.put(sql, result);
    }

    /** Parses the given query string. */
    public ParsedResult parse(String sql) {
        return parserService.parse(sql);
    }

    List<ParsedResult> parseScript(String sql) {
        return parserService.parseScript(sql);
    }

    void execute(Runnable runnable) {
        executor.execute(runnable);
    }

    HybridTimestamp deriveOperationTime(QueryTransactionContext txContext) {
        QueryTransactionWrapper txWrapper = txContext.explicitTx();

        if (txWrapper == null) {
            return clockService.now();
        }

        return txWrapper.unwrap().startTimestamp();
    }

    CompletableFuture<Void> waitForMetadata(HybridTimestamp timestamp) {
        return schemaSyncService.waitForMetadataCompleteness(timestamp);
    }

    CompletableFuture<QueryPlan> prepare(ParsedResult result, SqlOperationContext operationContext) {
        return prepareService.prepareAsync(result, operationContext);
    }

    HybridTimestamp deriveMinimalRequiredTime(QueryPlan plan) {
        Integer catalogVersion = null;

        if (plan instanceof MultiStepPlan) {
            catalogVersion = ((MultiStepPlan) plan).catalogVersion();
        } else if (plan instanceof KeyValueModifyPlan) {
            catalogVersion = ((KeyValueModifyPlan) plan).catalogVersion();
        } else if (plan instanceof KeyValueGetPlan) {
            catalogVersion = ((KeyValueGetPlan) plan).catalogVersion();
        }

        if (catalogVersion != null) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            assert catalog != null;

            return HybridTimestamp.hybridTimestamp(catalog.time());
        }

        return clockService.now();
    }

    AsyncDataCursor<InternalSqlRow> executePlan(
            SqlOperationContext ctx,
            QueryPlan plan
    ) {
        return executionService.executePlan(plan, ctx);
    }

    HybridTimestamp clockNow() {
        return clockService.now();
    }

    MultiStatementHandler createScriptHandler(Query query) {
        List<ParsedResult> parsedResults = query.parsedScript;

        assert parsedResults != null;

        return new MultiStatementHandler(
                transactionTracker,
                query,
                query.txContext,
                parsedResults,
                query.params
        );
    }

    private void trackQuery(Query query) {
        Query old = runningQueries.put(query.id, query);

        assert old == null : "Query with the same id already registered";

        query.onPhaseStarted(ExecutionPhase.TERMINATED)
                .whenComplete((ignored, ex) -> runningQueries.remove(query.id));
    }

    /** Returns list of queries registered on server at the moment. */
    public List<QueryInfo> runningQueries() {
        return runningQueries.values().stream()
                .map(QueryInfo::new)
                .collect(Collectors.toList());
    }

    @Override
    public void start() {
        // no-op
    }

    @Override
    public void stop() throws Exception {
        busyLock.block();

        Exception ex = new NodeStoppingException();

        runningQueries.values().forEach(query -> query.onError(ex));
    }
}
