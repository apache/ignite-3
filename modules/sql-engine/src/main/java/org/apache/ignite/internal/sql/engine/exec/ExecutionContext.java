/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.sql.engine.util.Commons.checkRange;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.extension.SqlExtension;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.AbstractQueryContext;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Runtime context allowing access to the tables in a database.
 */
public class ExecutionContext<RowT> extends AbstractQueryContext implements DataContext {
    private static final TimeZone TIME_ZONE = TimeZone.getDefault(); // TODO DistributedSqlConfiguration#timeZone

    /**
     * TODO: https://issues.apache.org/jira/browse/IGNITE-15276 Support other locales.
     */
    private static final Locale LOCALE = Locale.ENGLISH;

    private final BaseQueryContext qctx;

    private final QueryTaskExecutor executor;

    private final UUID qryId;

    private final FragmentDescription fragmentDesc;

    private final Map<String, Object> params;

    private final String locNodeId;

    private final String originatingNodeId;

    private final long topVer;

    private final RowHandler<RowT> handler;

    private final ExpressionFactory<RowT> expressionFactory;

    private final AtomicBoolean cancelFlag = new AtomicBoolean();

    /**
     * Need to store timestamp, since SQL standard says that functions such as CURRENT_TIMESTAMP return the same value throughout the
     * query.
     */
    private final long startTs;

    private Object[] correlations = new Object[16];

    /**
     * Constructor.
     *
     * @param executor     Task executor.
     * @param qctx         Base query context.
     * @param qryId        Query ID.
     * @param fragmentDesc Partitions information.
     * @param handler      Row handler.
     * @param params       Parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ExecutionContext(
            BaseQueryContext qctx,
            QueryTaskExecutor executor,
            UUID qryId,
            String locNodeId,
            String originatingNodeId,
            long topVer,
            FragmentDescription fragmentDesc,
            RowHandler<RowT> handler,
            Map<String, Object> params
    ) {
        super(qctx);

        this.executor = executor;
        this.qctx = qctx;
        this.qryId = qryId;
        this.fragmentDesc = fragmentDesc;
        this.handler = handler;
        this.params = params;
        this.locNodeId = locNodeId;
        this.originatingNodeId = originatingNodeId;
        this.topVer = topVer;

        expressionFactory = new ExpressionFactoryImpl<>(
                this,
                this.qctx.typeFactory(),
                this.qctx.config().getParserConfig().conformance()
        );

        long ts = System.currentTimeMillis();
        startTs = ts + TIME_ZONE.getOffset(ts);
    }

    /**
     * Get query ID.
     */
    public UUID queryId() {
        return qryId;
    }

    /**
     * Get fragment ID.
     */
    public long fragmentId() {
        return fragmentDesc.fragmentId();
    }

    /**
     * Get target mapping.
     */
    public ColocationGroup target() {
        return fragmentDesc.target();
    }

    /**
     * Get remote nodes for the given exchange id.
     *
     * @param exchangeId ExchangeId to find remote nodes for.
     * @return Remote nodes for given exchangeId, or {@code null}, when remote nodes not found for given exchangeId.
     */
    @Nullable
    public List<String> remotes(long exchangeId) {
        return fragmentDesc.remotes().get(exchangeId);
    }

    /**
     * Get colocation group for the given source id.
     *
     * @param sourceId SourceId to find colocation group for.
     * @return Colocation group for given sourceId.
     */
    public ColocationGroup group(long sourceId) {
        return fragmentDesc.mapping().findGroup(sourceId);
    }

    /**
     * Get keep binary flag.
     */
    public boolean keepBinary() {
        return true; // TODO
    }

    /**
     * Get handler to access row fields.
     */
    public RowHandler<RowT> rowHandler() {
        return handler;
    }

    /**
     * Get expression factory.
     */
    public ExpressionFactory<RowT> expressionFactory() {
        return expressionFactory;
    }

    /**
     * Get originating node ID.
     */
    public String originatingNodeId() {
        return originatingNodeId;
    }

    /**
     * Get local node ID.
     */
    public String localNodeId() {
        return locNodeId;
    }

    /**
     * Get topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * Get an extensions by it's name.
     *
     * @return An extensions or {@code null} if there is no extension with given name.
     */
    public @Nullable SqlExtension extension(String name) {
        return qctx.extension(name);
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus getRootSchema() {
        return qctx.schema();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTypeFactory getTypeFactory() {
        return qctx.typeFactory();
    }

    /** {@inheritDoc} */
    @Override
    public QueryProvider getQueryProvider() {
        return null; // TODO
    }

    /** {@inheritDoc} */
    @Override
    public Object get(String name) {
        if (Variable.CANCEL_FLAG.camelName.equals(name)) {
            return cancelFlag;
        }
        if (Variable.TIME_ZONE.camelName.equals(name)) {
            return TIME_ZONE; // TODO DistributedSqlConfiguration#timeZone
        }
        if (Variable.CURRENT_TIMESTAMP.camelName.equals(name)) {
            return startTs;
        }
        if (Variable.LOCAL_TIMESTAMP.camelName.equals(name)) {
            return startTs;
        }

        if (Variable.LOCALE.camelName.equals(name)) {
            return LOCALE;
        }

        if (name.startsWith("?")) {
            return TypeUtils.toInternal(this, params.get(name));
        }

        return params.get(name);
    }

    /**
     * Gets correlated value.
     *
     * @param id Correlation ID.
     * @return Correlated value.
     */
    public @NotNull Object getCorrelated(int id) {
        checkRange(correlations, id);

        return correlations[id];
    }

    /**
     * Sets correlated value.
     *
     * @param id    Correlation ID.
     * @param value Correlated value.
     */
    public void setCorrelated(@NotNull Object value, int id) {
        correlations = Commons.ensureCapacity(correlations, id + 1);

        correlations[id] = value;
    }

    /**
     * Executes a query task.
     *
     * @param task Query task.
     */
    public void execute(RunnableX task, Consumer<Throwable> onError) {
        if (isCancelled()) {
            return;
        }

        executor.execute(qryId, fragmentId(), () -> {
            try {
                task.run();
            } catch (Throwable e) {
                onError.accept(e);

                throw new IgniteInternalException("Unexpected exception", e);
            }
        });
    }

    /**
     * Submits a Runnable task for execution and returns a Future representing that task. The Future's {@code get} method will return {@code
     * null} upon <em>successful</em> completion.
     *
     * @param task the task to submit.
     * @return a {@link CompletableFuture} representing pending task
     */
    public CompletableFuture<?> submit(RunnableX task, Consumer<Throwable> onError) {
        assert !isCancelled() : "Call submit after execution was cancelled.";

        return executor.submit(qryId, fragmentId(), () -> {
            try {
                task.run();
            } catch (Throwable e) {
                onError.accept(e);

                throw new IgniteInternalException("Unexpected exception", e);
            }
        });
    }

    /**
     * RunnableX interface.
     */
    @FunctionalInterface
    public interface RunnableX {
        void run() throws Throwable;
    }

    /**
     * Sets cancel flag, returns {@code true} if flag was changed by this call.
     *
     * @return {@code True} if flag was changed by this call.
     */
    public boolean cancel() {
        return !cancelFlag.get() && cancelFlag.compareAndSet(false, true);
    }

    public boolean isCancelled() {
        return cancelFlag.get();
    }
}
