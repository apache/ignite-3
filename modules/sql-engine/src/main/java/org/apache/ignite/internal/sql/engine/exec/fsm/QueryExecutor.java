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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.Debuggable;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.QueryEventsFactory;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor.CancellationReason;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.TransactionalOperationTracker;
import org.apache.ignite.internal.sql.engine.prepare.DdlPlan;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueGetPlan;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueModifyPlan;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Executor which accepts requests for query execution and returns cursor to the result of execution.
 */
public class QueryExecutor implements LifecycleAware, Debuggable {
    private final Cache<String, ParsedResult> queryToParsedResultCache;
    private final ParserService parserService;
    private final Executor executor;
    private final ScheduledExecutorService scheduler;
    private final ClockService clockService;
    private final SchemaSyncService schemaSyncService;
    private final PrepareService prepareService;
    private final CatalogService catalogService;
    private final ExecutionService executionService;
    private final TransactionalOperationTracker transactionalOperationTracker;
    private final QueryIdGenerator idGenerator;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ConcurrentMap<UUID, Query> runningQueries = new ConcurrentHashMap<>();

    private final EventLog eventLog;

    private final QueryEventsFactory eventsFactory;

    private final SqlQueryMetricSource queryMetricSource;

    /**
     * Creates executor.
     *
     * @param nodeId Local node consistent ID.
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
     * @param transactionalOperationTracker Tracker to track usage of transactions by query.
     * @param idGenerator Id generator used to provide cluster-wide unique query id.
     * @param eventLog Event log.
     * @param queryMetricSource Query metric source.
     */
    public QueryExecutor(
            String nodeId,
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
            TransactionalOperationTracker transactionalOperationTracker,
            QueryIdGenerator idGenerator,
            EventLog eventLog,
            SqlQueryMetricSource queryMetricSource
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
        this.transactionalOperationTracker = transactionalOperationTracker;
        this.idGenerator = idGenerator;
        this.eventLog = eventLog;
        this.queryMetricSource = queryMetricSource;
        this.eventsFactory = new QueryEventsFactory(nodeId);
    }

    /**
     * Executes the given query with provided parameters.
     *
     * <p>This is a common entry point for both single statement and script execution.
     *
     * @param properties User query properties.
     * @param txContext Transactional context to use.
     * @param sql Query string.
     * @param cancellationToken Cancellation token.
     * @param params Query parameters.
     * @return Future which will be completed with cursor.
     */
    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeQuery(
            SqlProperties properties,
            QueryTransactionContext txContext,
            String sql,
            @Nullable CancellationToken cancellationToken,
            Object... params
    ) {
        Query query = new Query(
                Instant.ofEpochMilli(clockService.now().getPhysical()),
                this,
                idGenerator.next(),
                sql,
                properties,
                txContext,
                params
        );

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            trackQuery(query, cancellationToken);
        } finally {
            busyLock.leaveBusy();
        }

        long queryTimeout = properties.queryTimeout();

        if (queryTimeout > 0) {
            query.cancel.setTimeout(scheduler, queryTimeout);
        }

        return query.runProgram(Programs.QUERY_EXECUTION)
                .whenComplete((cursor, ex) -> {
                    if (cursor != null && query.parsedScript == null) {
                        cursor.onClose().thenRun(query::terminate);
                    }
                });
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
                idGenerator.next(),
                scriptTxContext,
                params,
                nextCursorFuture
        );

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            trackQuery(query, null);
        } finally {
            busyLock.leaveBusy();
        }

        try {
            parent.cancel.attach(query.cancel);
        } catch (QueryCancelledException ex) {
            query.terminate();

            return failedFuture(ex);
        }

        return query.runProgram(Programs.SCRIPT_ITEM_EXECUTION)
                .whenComplete((cursor, ex) -> {
                    if (cursor != null) {
                        cursor.onClose().thenRun(query::terminate);
                    } else if (ex != null) {
                        query.terminate();
                    }
                });
    }

    CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeChildBatch(
            Query parent,
            QueryTransactionContext scriptTxContext,
            int batchOffset,
            List<ParsedResultWithNextCursorFuture> batch
    ) {
        if (IgniteUtils.assertionsEnabled()) {
            int offsetInBatch = 0;
            for (ParsedResultWithNextCursorFuture item : batch) {
                assert item.parsedQuery.queryType() == SqlQueryType.DDL
                        : item.parsedQuery.queryType() + " at statement #" + (batchOffset + offsetInBatch);

                offsetInBatch++;
            }
        }

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        List<Query> queries = new ArrayList<>(batch.size());

        try {
            int offsetInBatch = 0;
            for (ParsedResultWithNextCursorFuture item : batch) {
                Query query = new Query(
                        Instant.ofEpochMilli(clockService.now().getPhysical()),
                        parent,
                        item.parsedQuery,
                        batchOffset + offsetInBatch,
                        idGenerator.next(),
                        scriptTxContext,
                        ArrayUtils.OBJECT_EMPTY_ARRAY,
                        item.nextCursorFuture
                );

                offsetInBatch++;

                trackQuery(query, null);

                queries.add(query);

                parent.cancel.attach(query.cancel);
            }
        } catch (QueryCancelledException ex) {
            queries.forEach(query -> query.terminateExceptionally(ex));

            return failedFuture(ex);
        } finally {
            busyLock.leaveBusy();
        }

        List<CompletableFuture<?>> preparedQueryFutures = new ArrayList<>(batch.size());
        for (Query query : queries) {
            preparedQueryFutures.add(query.runProgram(Programs.SCRIPT_ITEM_PREPARATION));
        }

        return CompletableFutures.allOf(preparedQueryFutures)
                .handle((none, ignored) -> {
                    List<DdlPlan> ddlPlans = new ArrayList<>();
                    for (Query query : queries) {
                        QueryPlan plan = query.plan;
                        if (plan == null) {
                            assert query.error.get() != null;

                            break;
                        }

                        ddlPlans.add((DdlPlan) plan);
                    }

                    return ddlPlans;
                })
                .thenCompose(ddlPlans -> {
                    parent.cancel.throwIfCancelled();

                    // First-statement-error case.
                    if (ddlPlans.isEmpty()) {
                        Iterator<Query> it = queries.iterator();

                        Throwable th = it.next().error.get();

                        assert th != null;

                        while (it.hasNext()) {
                            it.next().terminateExceptionally(th);
                        }

                        return failedFuture(th);
                    }

                    return executionService.executeDdlBatch(ddlPlans, scriptTxContext::updateObservableTime)
                            .handle((dataCursors, error) -> {
                                if (error != null) {
                                    // Fallback to statement-by-statement execution
                                    return executeSequentially(queries);
                                }

                                AsyncSqlCursor<InternalSqlRow> firstCursor = null;
                                CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorRef = null;
                                Iterator<AsyncDataCursor<InternalSqlRow>> dataCursorIterator = dataCursors.iterator();
                                Throwable th = null;
                                for (Query query : queries) {
                                    if (th != null) {
                                        query.terminateExceptionally(th);

                                        continue;
                                    }

                                    QueryPlan plan = query.plan;

                                    if (plan == null) {
                                        th = query.error.get();

                                        assert th != null;

                                        // First-statement-error case is covered before we call executionService,
                                        // and after first iteration cursorRef must not be null.
                                        assert cursorRef != null;

                                        cursorRef.completeExceptionally(th);

                                        continue;
                                    }

                                    query.moveTo(ExecutionPhase.EXECUTING);

                                    AsyncDataCursor<InternalSqlRow> dataCursor = dataCursorIterator.next();
                                    AsyncSqlCursor<InternalSqlRow> currentCursor = createAndSaveSqlCursor(query, dataCursor);

                                    if (cursorRef != null) {
                                        cursorRef.complete(currentCursor);
                                    }

                                    cursorRef = query.nextCursorFuture;

                                    if (firstCursor == null) {
                                        firstCursor = currentCursor;
                                    }

                                    currentCursor.onClose().thenRun(query::terminate);
                                }

                                return completedFuture(firstCursor);
                            })
                            .thenCompose(Function.identity());
                });
    }

    @SuppressWarnings("MethodMayBeStatic")
    private CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeSequentially(List<Query> queries) {
        assert !queries.isEmpty();

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> firstCursor = null;
        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorRef = null;
        CompletableFuture<Void> lastStep = nullCompletedFuture();
        for (Query query : queries) {
            query.reset();

            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorRef0 = cursorRef;
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture = lastStep
                    .whenComplete((none, ex) -> {
                        if (ex != null) {
                            query.terminateExceptionally(ex);
                        }
                    })
                    .thenCompose(none -> query.runProgram(Programs.SCRIPT_ITEM_EXECUTION)
                            .whenComplete((cursor, ex) -> {
                                if (cursorRef0 != null) {
                                    if (cursor != null) {
                                        cursorRef0.complete(cursor);
                                    } else {
                                        cursorRef0.completeExceptionally(ex);
                                    }
                                }

                                if (cursor != null) {
                                    cursor.onClose().thenRun(query::terminate);
                                } else if (ex != null) {
                                    query.terminate();
                                }
                            }));

            lastStep = cursorFuture.thenCompose(AsyncDataCursor::onFirstPageReady);
            cursorRef = query.nextCursorFuture;

            if (firstCursor == null) {
                firstCursor = cursorFuture;
            }
        }

        return firstCursor;
    }

    @SuppressWarnings("MethodMayBeStatic")
    AsyncSqlCursor<InternalSqlRow> createAndSaveSqlCursor(Query query, AsyncDataCursor<InternalSqlRow> dataCursor) {
        QueryPlan plan = query.plan;

        assert plan != null;

        AsyncSqlCursorImpl<InternalSqlRow> cursor = new AsyncSqlCursorImpl<>(
                plan.type(),
                plan.metadata(),
                plan.partitionAwarenessMetadata(),
                dataCursor,
                query.nextCursorFuture
        );

        query.cursor = cursor;

        query.cancel.add(timeout -> dataCursor.cancelAsync(
                timeout ? CancellationReason.TIMEOUT : CancellationReason.CANCEL
        ));

        return cursor;
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

        return txWrapper.unwrap().schemaTimestamp();
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

    CompletableFuture<AsyncDataCursor<InternalSqlRow>> executePlan(
            SqlOperationContext ctx,
            QueryPlan plan
    ) {
        return executionService.executePlan(plan, ctx);
    }

    MultiStatementHandler createScriptHandler(Query query) {
        List<ParsedResult> parsedResults = query.parsedScript;

        assert parsedResults != null;

        return new MultiStatementHandler(
                transactionalOperationTracker,
                query,
                query.txContext,
                parsedResults,
                query.params
        );
    }

    private void trackQuery(Query query, @Nullable CancellationToken cancellationToken) {
        Query old = runningQueries.put(query.id, query);

        eventLog.log(IgniteEventType.QUERY_STARTED.name(),
                () -> eventsFactory.makeStartEvent(new QueryInfo(query), EventUser.system()));

        assert old == null : "Query with the same id already registered";

        CompletableFuture<Void> queryTerminationFut = query.terminationFuture;

        queryTerminationFut.whenComplete((none, ignoredEx) -> {
            runningQueries.remove(query.id);

            long finishTime = clockService.current().getPhysical();

            updateMetrics(query);

            eventLog.log(IgniteEventType.QUERY_FINISHED.name(),
                    () -> eventsFactory.makeFinishEvent(new QueryInfo(query), EventUser.system(), finishTime));
        });

        if (cancellationToken != null) {
            CancelHandleHelper.addCancelAction(cancellationToken, query::cancel, queryTerminationFut);
        }
    }

    /** Returns list of queries registered on server at the moment. */
    public List<QueryInfo> runningQueries() {
        return runningQueries.values().stream()
                .map(QueryInfo::new)
                .collect(Collectors.toList());
    }

    /** Aborts the query with the given query ID. */
    public CompletableFuture<Boolean> cancelQuery(UUID queryId) {
        Query query = runningQueries.get(queryId);

        if (query == null) {
            return CompletableFutures.falseCompletedFuture();
        }

        return query.cancel().thenApply(none -> Boolean.TRUE);
    }

    @Override
    public void start() {
        // no-op
    }

    @Override
    public void stop() throws Exception {
        busyLock.block();

        Exception ex = new NodeStoppingException();

        runningQueries.values().forEach(query -> query.terminateExceptionally(ex));
    }

    static class ParsedResultWithNextCursorFuture {
        private final ParsedResult parsedQuery;
        private final @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCursorFuture;

        ParsedResultWithNextCursorFuture(
                ParsedResult parsedQuery,
                @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCursorFuture
        ) {
            this.parsedQuery = parsedQuery;
            this.nextCursorFuture = nextCursorFuture;
        }
    }

    @Override
    @TestOnly
    public void dumpState(IgniteStringBuilder writer, String indent) {
        writer.app(indent).app("Running queries:").nl();

        String childIndent = Debuggable.childIndentation(indent);
        for (Query query : runningQueries.values()) {
            writer.app(childIndent)
                    .app("queryId=").app(query.id)
                    .app(", phase=").app(query.currentPhase());

            if (query.parentId != null) {
                writer.app(", parentId=").app(query.parentId);
                writer.app(", statement=").app(query.statementNum);
            }

            writer.app(", createdAt=").app(query.createdAt)
                    .app(", cancelled=").app(query.cancel.isCancelled())
                    .app(", failed=").app(query.error.get() != null)
                    .app(", sql=").app(query.sql)
                    .nl();
        }

        if (executionService instanceof Debuggable) {
            ((Debuggable) executionService).dumpState(writer, indent);
        }
    }

    private void updateMetrics(Query query) {
        boolean individualStatement = query.parsedScript == null;
        // Ignore 'script' queries from metrics as well, because they act as containers for individual statements.
        if (!individualStatement) {
            return;
        }

        Throwable err = query.error.get();

        if (query.parsedResult == null || query.plan == null) {
            updateFailureMetrics(err);
        } else {
            if (err == null) {
                queryMetricSource.success();
            } else {
                updateFailureMetrics(err);
            }
        }
    }

    private void updateFailureMetrics(Throwable t) {
        queryMetricSource.failure();

        if (t instanceof QueryCancelledException) {
            if (QueryCancelledException.TIMEOUT_MSG.equals(t.getMessage())) {
                queryMetricSource.timedOut();
            } else {
                queryMetricSource.cancel();
            }
        }
    }
}
