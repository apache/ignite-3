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

package org.example.jobs.embedded;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;

/** Compute job that executes a long-running SQL query with the context's cancellation token. */
public class SqlQueryWithCancellationTokenJob implements ComputeJob<Void, Void> {
    @Override
    public CompletableFuture<Void> executeAsync(JobExecutionContext context, Void arg) {
        // Execute a long-running SQL query using the job's cancellation token.
        // When the job is cancelled, the cancellation token propagates to the SQL query.
        return context.ignite().sql()
                .executeAsync((Transaction) null, context.cancellationToken(),
                        "SELECT * FROM system_range(0, 10000000000)")
                .thenCompose(SqlQueryWithCancellationTokenJob::drainPages)
                .thenApply(v -> null);
    }

    private static CompletableFuture<Void> drainPages(AsyncResultSet<?> rs) {
        if (!rs.hasMorePages()) {
            return rs.closeAsync();
        }

        return rs.fetchNextPage().thenCompose(SqlQueryWithCancellationTokenJob::drainPages);
    }
}
