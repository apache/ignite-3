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

import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor.PrefetchCallback;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Context of the sql operation.
 *
 * <p>Encloses parameters provided by user as well as default substitutions of omitted parameters required for initialization and
 * kick-starting of the statement execution.
 */
public final class SqlOperationContext {
    private final UUID queryId;
    private final ZoneId timeZoneId;
    private final Object[] parameters;
    private final HybridTimestamp operationTime;
    private final QueryTransactionContext txContext;
    private final Instant operationDeadline;

    private final @Nullable QueryCancel cancel;
    private final @Nullable String defaultSchemaName;
    private final @Nullable PrefetchCallback prefetchCallback;
    private final CompletableFuture<Void> timeoutFuture;

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
            @Nullable PrefetchCallback prefetchCallback,
            @Nullable Instant deadline,
            @Nullable CompletableFuture<Void> timeoutFuture
    ) {
        this.queryId = queryId;
        this.timeZoneId = timeZoneId;
        this.parameters = parameters;
        this.operationTime = operationTime;
        this.txContext = txContext;
        this.cancel = cancel;
        this.defaultSchemaName = defaultSchemaName;
        this.prefetchCallback = prefetchCallback;
        this.operationDeadline = deadline;
        this.timeoutFuture = timeoutFuture;
    }

    public static Builder builder() {
        return new Builder();
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
     * Returns callback to notify about readiness of the first page of the results.
     *
     * <p>May be null on remote side, but never null on node initiator.
     */
    public @Nullable PrefetchCallback prefetchCallback() {
        return prefetchCallback;
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
     * Returns the operation time.
     *
     * <p>The time the operation started is the logical time it runs, and all the time readings during the execution time as well as all
     * time-related operations (like acquiring an active schema) should reflect the same time.
     */
    public HybridTimestamp operationTime() {
        return operationTime;
    }

    /**
     * Returns the deadline of the operation.
     *
     * <p>Can be null if a query has no timeout or a node is not a query initiator.
     */
    public @Nullable Instant operationDeadline() {
        return operationDeadline;
    }

    /**
     * Returns the timeout future.
     *
     * <p>Can be null if a query has no timeout or a node is not a query initiator.
     */
    public @Nullable CompletableFuture<Void> timeoutFuture() {
        return timeoutFuture;
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
        private @Nullable Instant operationDeadline;

        private @Nullable QueryCancel cancel;
        private @Nullable String defaultSchemaName;
        private @Nullable PrefetchCallback prefetchCallback;
        private @Nullable CompletableFuture<Void> timeoutFut;

        public Builder cancel(@Nullable QueryCancel cancel) {
            this.cancel = requireNonNull(cancel);
            return this;
        }

        public Builder queryId(UUID queryId) {
            this.queryId = requireNonNull(queryId);
            return this;
        }

        public Builder prefetchCallback(@Nullable PrefetchCallback prefetchCallback) {
            this.prefetchCallback = prefetchCallback;
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

        public Builder operationDeadline(@Nullable Instant operationDeadline) {
            this.operationDeadline = operationDeadline;
            return this;
        }

        public Builder timeoutFuture(@Nullable CompletableFuture<Void> timeoutFut) {
            this.timeoutFut = timeoutFut;
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
                    prefetchCallback,
                    operationDeadline,
                    timeoutFut
            );
        }
    }
}
