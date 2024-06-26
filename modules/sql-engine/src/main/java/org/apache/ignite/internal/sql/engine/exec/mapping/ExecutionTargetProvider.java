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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;

/**
 * An integration point that helps the mapper to acquire an execution target of particular
 * relation from fragment.
 */
public interface ExecutionTargetProvider {
    /**
     * Returns an execution target for a given table.
     *
     * @param operationTime Time of the operation to get consistent results among different calls.
     * @param factory A factory to create target for given table.
     * @param table A table to create execution target for.
     * @param includeBackups Flags denotes whether to include non-primary replicas into target.
     * @return A future representing the result.
     */
    CompletableFuture<ExecutionTarget> forTable(
            HybridTimestamp operationTime,
            ExecutionTargetFactory factory,
            IgniteTable table,
            boolean includeBackups
    );

    /**
     * Returns an execution target for a given view.
     *
     * @param factory A factory to create target for given table.
     * @param view A view to create execution target for.
     * @return A future representing the result.
     */
    CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view);
}
