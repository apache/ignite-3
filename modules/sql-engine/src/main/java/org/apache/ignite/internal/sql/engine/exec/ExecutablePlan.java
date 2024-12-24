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

import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryPrefetchCallback;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.jetbrains.annotations.Nullable;

/**
 * Denotes a plan that can evaluates itself.
 */
@FunctionalInterface
public interface ExecutablePlan {
    /**
     * Evaluates plan and returns cursor over result.
     *
     * @param ctx An execution context.
     * @param tx A transaction to use to access the data.
     * @param tableRegistry A registry to resolve executable table to evaluate the plan.
     * @param firstPageReadyCallback A callback to notify when first page has been prefetched.
     * @param <RowT> A type of the sql row.
     * @return Cursor over result of the evaluation.
     */
    <RowT> AsyncCursor<InternalSqlRow> execute(
            ExecutionContext<RowT> ctx,
            InternalTransaction tx,
            ExecutableTableRegistry tableRegistry,
            @Nullable QueryPrefetchCallback firstPageReadyCallback
    );
}
