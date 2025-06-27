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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.prepare.DdlPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;

/**
 * SQL query plan execution interface.
 */
public interface ExecutionService extends LifecycleAware {
    /**
     * Executes the given plan.
     *
     * @param plan Plan to execute.
     * @param operationContext Context of operation.
     * @return Future that will be completed when cursor is successfully initialized, implying for distributed plans all fragments have been
     *         sent successfully.
     */
    CompletableFuture<AsyncDataCursor<InternalSqlRow>> executePlan(
            QueryPlan plan, SqlOperationContext operationContext
    );

    /**
     * Executes given batch of {@link DdlPlan}s atomically.
     *
     * <p>The whole batch will be executed at once. If exception arises during execution of any plan within the batch, then
     * none of the changes will be saved.
     *
     * @param batch A batch to execute.
     * @param activationTimeListener A listener to notify with activation time of catalog in which all changes become visible.
     * @return A future containing a list of cursors representing result of execution, one per each command.
     */
    CompletableFuture<List<AsyncDataCursor<InternalSqlRow>>> executeDdlBatch(
            List<DdlPlan> batch,
            Consumer<HybridTimestamp> activationTimeListener
    );
}
