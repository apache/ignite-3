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

package org.apache.ignite.client.handler.requests.sql;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;

/**
 * Client result set wrapper.
 */
class ClientSqlResultSet {
    /** Result set. */
    private final AsyncResultSet<SqlRow> resultSet;

    /** Metrics. */
    private final ClientHandlerMetricSource metrics;

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param resultSet Result set.
     * @param metrics Metrics.
     */
    ClientSqlResultSet(AsyncResultSet<SqlRow> resultSet, ClientHandlerMetricSource metrics) {
        assert resultSet != null;
        assert metrics != null;

        this.resultSet = resultSet;
        this.metrics = metrics;
    }

    /**
     * Gets the result set.
     *
     * @return Result set.
     */
    public AsyncResultSet<SqlRow> resultSet() {
        return resultSet;
    }

    /**
     * Closes underlying result set and session.
     *
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> closeAsync() {
        if (closed.compareAndSet(false, true)) {
            metrics.cursorsActiveDecrement();

            return resultSet.closeAsync();
        }

        return nullCompletedFuture();
    }
}
