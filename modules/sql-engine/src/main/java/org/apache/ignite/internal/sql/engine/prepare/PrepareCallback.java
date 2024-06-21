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

import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.jetbrains.annotations.Nullable;

/**
 * Callback to be notified on preparation steps.
 */
public interface PrepareCallback {

    /**
     * Called for a valid SQL tree prior to optimization.
     *
     * @param ctx Planning context.
     * @param node Validated SQL tree.
     */
    default void beforeOptimization(PlanningContext ctx, SqlNode node) {
        // No-op.
    }

    /**
     * Called for a valid SQL tree after optimization step completes. When optimization step completes successfully called with
     * {@code optimizedRel} not null and {@code err} null. When optimization step completes with an error called with {@code optimizedRel}
     * null and {@code err} not null.
     *
     * @param ctx Planning context.
     * @param node Validated SQL tree.
     * @param optimizedRel Optimized relational tree, if optimization was successful.
     * @param err Error that occurred during optimization, if any.
     */
    default void afterOptimization(PlanningContext ctx, SqlNode node, @Nullable IgniteRel optimizedRel, @Nullable Throwable err) {
        // No-op.
    }
}
