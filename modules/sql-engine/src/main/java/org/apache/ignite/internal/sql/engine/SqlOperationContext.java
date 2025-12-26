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

import static java.util.Objects.requireNonNull;

import java.time.ZoneId;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Context of the sql operation.
 *
 * <p>Encloses parameters provided by user as well as default substitutions of omitted parameters required for initialization and
 * kick-starting of the statement execution.
 */
public final class SqlOperationContext {
    private final Set<String> excludedNodes = ConcurrentHashMap.newKeySet();

    private final UUID queryId;
    private final ZoneId timeZoneId;
    private final Object[] parameters;
    private final HybridTimestamp operationTime;
    private final QueryTransactionContext txContext;

    private final @Nullable QueryCancel cancel;
    private final @Nullable String defaultSchemaName;
    private final @Nullable Consumer<QueryTransactionWrapper> txUsedListener;
    private final @Nullable Consumer<Throwable> errorListener;
    private final @Nullable String userName;
    private final @Nullable Long topologyVersion;
    private final AtomicReference<QueryTransactionWrapper> txOnRetryHolder;

    /**
     * Private constructor, used by a builder.
     */
    private SqlOperationContext(
            UUID queryId,
            ZoneId timeZoneId,
            Object[] parameters,
            HybridTimestamp operationTime,
            @Nullable QueryTransactionContext txContext,
            @Nullable QueryCancel cancel,
            @Nullable String defaultSchemaName,
            @Nullable Consumer<QueryTransactionWrapper> txUsedListener,
            @Nullable Consumer<Throwable> errorListener,
            @Nullable String userName,
            @Nullable Long topologyVersion,
            @Nullable QueryTransactionWrapper txOnRetry
    ) {
        this.queryId = queryId;
        this.timeZoneId = timeZoneId;
        this.parameters = parameters;
        this.operationTime = operationTime;
        this.txContext = txContext;
        this.cancel = cancel;
        this.defaultSchemaName = defaultSchemaName;
        this.txUsedListener = txUsedListener;
        this.errorListener = errorListener;
        this.userName = userName;
        this.topologyVersion = topologyVersion;
        this.txOnRetryHolder = new AtomicReference<>(txOnRetry);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Copies an existing context with a new operation time.
     *
     * <p>Used in case of a retry. If the operation is repeated while preserving the running transaction,
     *    the operation time is taken from this transaction.
     */
    public SqlOperationContext withOperationTime(Supplier<HybridTimestamp> operationTimeSupplier) {
        QueryTransactionWrapper retryTx = txOnRetryHolder.get();
        HybridTimestamp operationTime = retryTx == null
                ? operationTimeSupplier.get()
                : retryTx.unwrap().schemaTimestamp();

        return new SqlOperationContext(
                queryId,
                timeZoneId,
                parameters,
                operationTime,
                txContext,
                cancel,
                defaultSchemaName,
                txUsedListener,
                errorListener,
                userName,
                topologyVersion,
                retryTx
        );
    }

    /** Returns unique identifier of the query. */
    public UUID queryId() {
        return queryId;
    }

    /** Returns parameters provided by user required to execute the statement. May be empty but never null. */
    public Object[] parameters() {
        return parameters;
    }

    /**
     * Returns handler to cancel running operation, as well as add listeners to be notified about cancellation of the query.
     *
     * <p>May be null on remote side, but never null on node initiator.
     */
    public @Nullable QueryCancel cancel() {
        return cancel;
    }

    /** Returns zone id provided by user, to adjust temporal values to match user's time zone during to string conversion. */
    public ZoneId timeZoneId() {
        return timeZoneId;
    }

    /** Returns current user name or {@code null} if unknown. */
    public @Nullable String userName() {
        return userName;
    }

    /**
     * Returns name of the schema to use to resolve schema objects, like tables or system views, for which name of the schema was omitted.
     *
     * <p>May be null on remote side, but never null on node initiator.
     */
    public @Nullable String defaultSchemaName() {
        return defaultSchemaName;
    }

    /**
     * Returns context to work with transaction.
     *
     * <p>May be null on remote side, but never null on node initiator.
     */
    public @Nullable QueryTransactionContext txContext() {
        return txContext;
    }

    /**
     * Returns topology version with query was mapped on.
     * 
     * <p>May be null, if the node is the initiator.
     */
    public @Nullable Long topologyVersion() {
        return topologyVersion;
    }

    /**
     * Notifies context that transaction was used for query execution.
     */
    public void notifyTxUsed(QueryTransactionWrapper tx) {
        if (txUsedListener != null) {
            txUsedListener.accept(tx);
        }
    }

    /**
     * Notifies the context that an error has occurred.
     */
    public void notifyError(Throwable th) {
        if (errorListener != null) {
            errorListener.accept(th);
        }
    }

    /**
     * Returns the operation time.
     *
     * <p>The time the operation started is the logical time it runs, and all the time readings during the execution time as well as all
     * time-related operations (like acquiring an active schema) should reflect the same time.
     */
    public HybridTimestamp operationTime() {
        return operationTime;
    }

    /** Updates the {@link #nodeExclusionFilter()} with given node. */
    public void excludeNode(String nodeName) {
        excludedNodes.add(nodeName);
    }

    /** Returns the predicate to exclude nodes from mapping, or {@code null} if all nodes must be used. */
    public @Nullable Predicate<String> nodeExclusionFilter() {
        Set<String> excludedNodes = Set.copyOf(this.excludedNodes);

        return excludedNodes.isEmpty() ? null : excludedNodes::contains;
    }

    /** Atomically updates the transaction tracked by the context. */
    public @Nullable QueryTransactionWrapper getAndSetTxOnRetry(@Nullable QueryTransactionWrapper usedTx) {
        return txOnRetryHolder.getAndSet(usedTx);
    }

    /**
     * Query context builder.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Builder {
        private UUID queryId;
        private ZoneId timeZoneId;
        private Object[] parameters = ArrayUtils.OBJECT_EMPTY_ARRAY;
        private HybridTimestamp operationTime;
        private @Nullable QueryTransactionContext txContext;

        private @Nullable Consumer<QueryTransactionWrapper> txUsedListener;
        private @Nullable Consumer<Throwable> errorListener;
        private @Nullable QueryCancel cancel;
        private @Nullable String defaultSchemaName;
        private @Nullable String userName;
        private @Nullable Long topologyVersion;

        public Builder cancel(@Nullable QueryCancel cancel) {
            this.cancel = requireNonNull(cancel);
            return this;
        }

        public Builder queryId(UUID queryId) {
            this.queryId = requireNonNull(queryId);
            return this;
        }

        public Builder parameters(Object... parameters) {
            this.parameters = requireNonNull(parameters);
            return this;
        }

        public Builder timeZoneId(ZoneId timeZoneId) {
            this.timeZoneId = timeZoneId;
            return this;
        }

        public Builder defaultSchemaName(@Nullable String defaultSchemaName) {
            this.defaultSchemaName = defaultSchemaName;
            return this;
        }

        public Builder operationTime(HybridTimestamp operationTime) {
            this.operationTime = operationTime;
            return this;
        }

        public Builder txContext(@Nullable QueryTransactionContext txContext) {
            this.txContext = txContext;
            return this;
        }

        public Builder txUsedListener(Consumer<QueryTransactionWrapper> txUsedListener) {
            this.txUsedListener = txUsedListener;
            return this;
        }

        public Builder errorHandler(Consumer<Throwable> errorListener) {
            this.errorListener = errorListener;
            return this;
        }

        public Builder userName(@Nullable String userName) {
            this.userName = userName;
            return this;
        }

        public Builder topologyVersion(@Nullable Long topologyVersion) {
            this.topologyVersion = topologyVersion;
            return this;
        }

        /** Creates new context. */
        public SqlOperationContext build() {
            return new SqlOperationContext(
                    requireNonNull(queryId, "queryId"),
                    requireNonNull(timeZoneId, "timeZoneId"),
                    requireNonNull(parameters, "parameters"),
                    requireNonNull(operationTime, "operationTime"),
                    txContext,
                    cancel,
                    defaultSchemaName,
                    txUsedListener,
                    errorListener,
                    userName,
                    topologyVersion,
                    null
            );
        }
    }
}
