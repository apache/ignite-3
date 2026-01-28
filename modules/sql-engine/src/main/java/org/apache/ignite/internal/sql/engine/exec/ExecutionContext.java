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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningColumns;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Runtime context allowing access to the tables in a database.
 */
public class ExecutionContext<RowT> implements SqlEvaluationContext<RowT> {
    private static final IgniteLogger LOG = Loggers.forClass(ExecutionContext.class);

    /**
     * TODO: https://issues.apache.org/jira/browse/IGNITE-15276 Support other locales.
     */
    private static final Locale LOCALE = Locale.ENGLISH;

    private final int inBufSize;

    private final QueryTaskExecutor executor;

    private final ExecutionId executionId;

    private final FragmentDescription description;

    private final Map<String, Object> params;

    private final InternalClusterNode localNode;

    private final String originatingNodeName;
    private final UUID originatingNodeId;

    private final RowHandler<RowT> handler;
    private final RowFactoryFactory<RowT> rowFactoryFactory;

    private final SqlExpressionFactory sqlExpressionFactory;

    private final AtomicBoolean cancelFlag = new AtomicBoolean();

    /**
     * Current timestamp. Need to store timestamp, since SQL standard says that functions such as CURRENT_TIMESTAMP return the same value
     * throughout the query.
     */
    private final long startTs;

    /**
     * Current timestamp that includes offset an offset of {@code timeZoneId}. Need to store timestamp, since SQL standard says that
     * functions such as CURRENT_TIMESTAMP return the same value throughout the query.
     */
    private final long startTsWithTzOffset;

    private final TxAttributes txAttributes;

    private final ZoneId timeZoneId;

    private final String currentUser;

    private SharedState sharedState = new SharedState();

    private final @Nullable Long topologyVersion;

    /**
     * Constructor.
     *
     * @param sqlExpressionFactory Expression factory.
     * @param executor Task executor.
     * @param executionId Execution ID.
     * @param localNode Local node.
     * @param originatingNodeName Name of the node that initiated the query.
     * @param description Partitions information.
     * @param handler Row handler.
     * @param rowFactoryFactory Factory that produces factories to create row..
     * @param params Parameters.
     * @param txAttributes Transaction attributes.
     * @param timeZoneId Session time-zone ID.
     * @param inBufSize Default execution nodes' internal buffer size. Negative value means default value.
     * @param clock The clock to use to get the system time.
     * @param username Authenticated user name or {@code null} for unknown user.
     * @param topologyVersion Topology version the query was mapped on.
     */
    public ExecutionContext(
            SqlExpressionFactory sqlExpressionFactory,
            QueryTaskExecutor executor,
            ExecutionId executionId,
            InternalClusterNode localNode,
            String originatingNodeName,
            UUID originatingNodeId,
            FragmentDescription description,
            RowHandler<RowT> handler,
            RowFactoryFactory<RowT> rowFactoryFactory,
            Map<String, Object> params,
            TxAttributes txAttributes,
            ZoneId timeZoneId,
            int inBufSize,
            Clock clock,
            @Nullable String username,
            @Nullable Long topologyVersion
    ) {
        this.sqlExpressionFactory = sqlExpressionFactory;
        this.executor = executor;
        this.executionId = executionId;
        this.description = description;
        this.handler = handler;
        this.rowFactoryFactory = rowFactoryFactory;
        this.params = params;
        this.localNode = localNode;
        this.originatingNodeName = originatingNodeName;
        this.originatingNodeId = originatingNodeId;
        this.txAttributes = txAttributes;
        this.timeZoneId = timeZoneId;
        this.inBufSize = inBufSize < 0 ? Commons.IN_BUFFER_SIZE : inBufSize;
        this.currentUser = username;
        this.topologyVersion = topologyVersion;

        assert this.inBufSize > 0 : this.inBufSize;

        Instant nowUtc = Instant.now(clock);
        startTs = nowUtc.toEpochMilli();
        startTsWithTzOffset = nowUtc.plusSeconds(this.timeZoneId.getRules().getOffset(nowUtc).getTotalSeconds()).toEpochMilli();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Context created [executionId={}, fragmentId={}]", executionId, fragmentId());
        }
    }

    /**
     * Get query ID.
     */
    public UUID queryId() {
        return executionId.queryId();
    }

    public int executionToken() {
        return executionId.executionToken();
    }

    public ExecutionId executionId() {
        return executionId;
    }

    /**
     * Get fragment ID.
     */
    public long fragmentId() {
        return description.fragmentId();
    }

    /**
     * Get target mapping.
     */
    public @Nullable ColocationGroup target() {
        return description.target();
    }

    public FragmentDescription description() {
        return description;
    }

    /**
     * Get remote nodes for the given exchange id.
     *
     * @param exchangeId ExchangeId to find remote nodes for.
     * @return Remote nodes for given exchangeId, or {@code null}, when remote nodes not found for given exchangeId.
     */
    @Nullable
    public List<String> remotes(long exchangeId) {
        return description.remotes(exchangeId);
    }

    /**
     * Get colocation group for the given source id.
     *
     * @param sourceId SourceId to find colocation group for.
     * @return Colocation group for given sourceId.
     */
    public @Nullable ColocationGroup group(long sourceId) {
        return description.group(sourceId);
    }

    @Override
    public RowHandler<RowT> rowAccessor() {
        return handler;
    }

    @Override
    public RowFactoryFactory<RowT> rowFactoryFactory() {
        return rowFactoryFactory;
    }

    /**
     * Get expression factory.
     */
    public SqlExpressionFactory expressionFactory() {
        return sqlExpressionFactory;
    }

    /**
     * Get originating node consistent ID.
     */
    public String originatingNodeName() {
        return originatingNodeName;
    }

    /**
     * Get originating node volatile ID.
     */
    public UUID originatingNodeId() {
        return originatingNodeId;
    }

    /**
     * Get local node.
     */
    public InternalClusterNode localNode() {
        return localNode;
    }

    /**
     * Gets buffer size that is used by execution nodes, which supports buffering.
     */
    public int bufferSize() {
        return inBufSize;
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus getRootSchema() {
        throw new AssertionError("should not be called");
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTypeFactory getTypeFactory() {
        return IgniteTypeFactory.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override
    public QueryProvider getQueryProvider() {
        throw new AssertionError("should not be called");
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object get(String name) {
        if (Variable.CANCEL_FLAG.camelName.equals(name)) {
            return cancelFlag;
        }
        if (Variable.CURRENT_TIMESTAMP.camelName.equals(name)) {
            return startTs;
        }
        if (Variable.LOCAL_TIMESTAMP.camelName.equals(name)) {
            return startTsWithTzOffset;
        }

        if (Variable.LOCALE.camelName.equals(name)) {
            return LOCALE;
        }

        if (Variable.TIME_ZONE.camelName.equals(name)) {
            return TimeZone.getTimeZone(timeZoneId);
        }
        if (Variable.USER.camelName.equals(name)) {
            return currentUser;
        }

        if (name.startsWith("?")) {
            return getParameter(name);
        } else {
            return params.get(name);
        }
    }

    /** Returns the topology version the query was mapped on. */
    public @Nullable Long topologyVersion() {
        return topologyVersion;
    }

    /** Gets dynamic parameters by name. */
    private @Nullable Object getParameter(String name) {
        assert name.startsWith("?") : name;

        Object param = params.get(name);

        if (param == null) {
            if (!params.containsKey(name)) {
                throw new IllegalStateException("Missing dynamic parameter: " + name);
            }

            return null;
        }

        NativeType nativeType = NativeTypes.fromObject(param);

        if (nativeType == null) {
            throw new IllegalArgumentException(format(
                    "Dynamic parameter of unsupported type: parameterName={}, type={}",
                    name,
                    param.getClass()
            ));
        }

        return TypeUtils.toInternal(param, nativeType.spec());
    }

    @Override
    public @Nullable Object correlatedVariable(long id) {
        return sharedState.correlatedVariable(id);
    }

    /**
     * Sets correlated value.
     *
     * @param id Correlation ID.
     * @param value Correlated value.
     */
    public void correlatedVariable(long id, @Nullable Object value) {
        sharedState.correlatedVariable(id, value);
    }

    /**
     * Updates the state in the context with the given one.
     *
     * @param state A state to update with.
     */
    public void sharedState(SharedState state) {
        sharedState = state;
    }

    /**
     * Returns the current state.
     *
     * @return Current state.
     */
    public SharedState sharedState() {
        return sharedState;
    }

    /**
     * Executes a query task. To execute a task from a {@link Node} use {@link Node#execute(RunnableX)} instead.
     *
     * @param task Query task.
     */
    public void execute(RunnableX task, Consumer<Throwable> onError) {
        if (isCancelled()) {
            return;
        }

        executor.execute(queryId(), fragmentId(), () -> {
            try {
                if (!isCancelled()) {
                    task.run();
                }
            } catch (Throwable e) {
                Throwable unwrappedException = ExceptionUtils.unwrapCause(e);
                onError.accept(unwrappedException);

                if (unwrappedException instanceof IgniteException
                        || unwrappedException instanceof IgniteInternalException
                        || unwrappedException instanceof IgniteCheckedException
                        || unwrappedException instanceof IgniteInternalCheckedException
                ) {
                    return;
                }

                LOG.warn("Unexpected exception", e);
            }
        });
    }

    /**
     * Submits a Runnable task for execution and returns a Future representing that task. The Future's {@code get} method will return
     * {@code null} upon <em>successful</em> completion.
     *
     * @param task the task to submit.
     * @return a {@link CompletableFuture} representing pending task
     */
    public CompletableFuture<?> submit(RunnableX task, Consumer<Throwable> onError) {
        assert !isCancelled() : "Call submit after execution was cancelled.";

        return executor.submit(queryId(), fragmentId(), () -> {
            try {
                task.run();
            } catch (Throwable e) {
                onError.accept(e);

                throw new IgniteInternalException(INTERNAL_ERR, "Unexpected exception", e);
            }
        });
    }

    /** Transaction for current context. */
    public TxAttributes txAttributes() {
        return txAttributes;
    }

    /**
     * Sets cancel flag, returns {@code true} if flag was changed by this call.
     *
     * @return {@code True} if flag was changed by this call.
     */
    public boolean cancel() {
        boolean res = !cancelFlag.get() && cancelFlag.compareAndSet(false, true);

        if (res && LOG.isTraceEnabled()) {
            LOG.trace("Context cancelled [executionId={}, fragmentId={}]", executionId, fragmentId());
        }

        return res;
    }

    public boolean isCancelled() {
        return cancelFlag.get();
    }

    /** Creates {@link PartitionProvider} for the given source table. */
    public PartitionProvider<RowT> getPartitionProvider(long sourceId, ColocationGroup group, IgniteTable table) {
        PartitionPruningMetadata metadata = description.partitionPruningMetadata();
        PartitionPruningColumns columns = metadata != null ? metadata.get(sourceId) : null;
        String nodeName = localNode.name();

        if (columns == null) {
            return new StaticPartitionProvider<>(nodeName, group, sourceId);
        } else {
            return new DynamicPartitionProvider<>(nodeName, group.assignments(), columns, table);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExecutionContext<?> context = (ExecutionContext<?>) o;

        return executionId.equals(context.executionId) && description.fragmentId() == context.description.fragmentId();
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(executionId, description.fragmentId());
    }
}
