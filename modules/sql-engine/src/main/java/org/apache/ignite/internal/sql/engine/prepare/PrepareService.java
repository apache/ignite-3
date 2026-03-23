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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * Preparation service that accepts an AST of the query and returns a prepared query plan.
 */
public interface PrepareService extends LifecycleAware {

    /**
     * Prepare query plan.
     *
     * @param parsedResult Represent of parsed query as AST, which need to be planned.
     * @param ctx Query context.
     *
     * @return Future that contains prepared query plan when completes.
     */
    CompletableFuture<QueryPlan> prepareAsync(ParsedResult parsedResult, SqlOperationContext ctx);

    /**
     * Invalidates planner cache if {@code tableNames} is empty, otherwise invalidates only plans, which refers to the provided tables.
     *
     * @param tableNames Table names.
     * @return Operation completion future.
     */
    default CompletableFuture<Void> invalidateCache(Set<String> tableNames) {
        return CompletableFutures.nullCompletedFuture();
    }

    /**
     * Returns prepared plans stored in cached.
     *
     * @return Cached prepared plans.
     */
    Set<PreparedPlan> preparedPlans();
}
