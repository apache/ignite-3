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

package org.apache.ignite.internal.sql.engine.util;

import static java.util.Objects.requireNonNull;

import java.time.ZoneId;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor.PrefetchCallback;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * Base query context.
 */
public final class SqlOperationContext {
    private final QueryCancel cancel;

    private final UUID queryId;

    private final Object[] parameters;

    private final PrefetchCallback prefetchCallback;

    private final ZoneId timeZoneId;

    private final HybridTimestamp operationTime;

    private final String defaultSchemaName;

    /**
     * Private constructor, used by a builder.
     */
    private SqlOperationContext(
            UUID queryId,
            HybridTimestamp operationTime,
            QueryCancel cancel,
            Object[] parameters,
            PrefetchCallback prefetchCallback,
            ZoneId timeZoneId,
            String defaultSchemaName
    ) {
        this.queryId = queryId;
        this.cancel = cancel;
        this.parameters = parameters;
        this.prefetchCallback = prefetchCallback;
        this.timeZoneId = timeZoneId;
        this.operationTime = operationTime;
        this.defaultSchemaName = defaultSchemaName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public UUID queryId() {
        return queryId;
    }

    public Object[] parameters() {
        return parameters;
    }

    public PrefetchCallback prefetchCallback() {
        return prefetchCallback;
    }

    public QueryCancel cancel() {
        return cancel;
    }

    public ZoneId timeZoneId() {
        return timeZoneId;
    }

    public String defaultSchemaName() {
        return defaultSchemaName;
    }

    public HybridTimestamp operationTime() {
        return operationTime;
    }

    /**
     * Query context builder.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Builder {
        private QueryCancel cancel = new QueryCancel();

        private UUID queryId;

        private Object[] parameters = ArrayUtils.OBJECT_EMPTY_ARRAY;

        private ZoneId timeZoneId;

        private PrefetchCallback prefetchCallback;

        private HybridTimestamp operationTime;

        private String defaultSchemaName;

        public Builder cancel(QueryCancel cancel) {
            this.cancel = requireNonNull(cancel);
            return this;
        }

        public Builder queryId(UUID queryId) {
            this.queryId = requireNonNull(queryId);
            return this;
        }

        public Builder prefetchCallback(PrefetchCallback prefetchCallback) {
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

        public Builder defaultSchemaName(String defaultSchemaName) {
            this.defaultSchemaName = defaultSchemaName;
            return this;
        }

        public Builder operationTime(HybridTimestamp operationTime) {
            this.operationTime = operationTime;
            return this;
        }

        /** Creates new context. */
        public SqlOperationContext build() {
            return new SqlOperationContext(
                    requireNonNull(queryId, "queryId"),
                    requireNonNull(operationTime, "operationTime"),
                    requireNonNull(cancel, "cancel"),
                    parameters,
                    prefetchCallback,
                    timeZoneId,
                    defaultSchemaName
            );
        }
    }
}
