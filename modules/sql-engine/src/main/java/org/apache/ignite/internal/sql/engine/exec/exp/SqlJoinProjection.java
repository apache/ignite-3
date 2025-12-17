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

package org.apache.ignite.internal.sql.engine.exec.exp;

import org.apache.ignite.internal.sql.engine.exec.SqlEvaluationContext;

/**
 * A functional interface representing a projection specifically designed for SQL join operations.
 *
 * <p>This interface defines a single method, {@link #project(SqlEvaluationContext, Object, Object)}, 
 * which computes projection based on two rows (the left and right rows) within the context of a join operation. 
 * It allows to fuse rows concatenation and consequent projection (for example, restoring original order of columns
 * after join input was switched), reducing the need for additional post-join transformations and optimizing
 * query execution.
 */
@FunctionalInterface
public interface SqlJoinProjection {
    /**
     * Applies a projection to the given execution rows.
     *
     * @param context The execution context, providing access to query-related data.
     * @param left The left row in the join operation.
     * @param right The right row in the join operation.
     * @param <RowT> The type of the execution row.
     * @return The projected row, which may be a modified version of the input rows or a new row.
     */
    <RowT> RowT project(SqlEvaluationContext<RowT> context, RowT left, RowT right);
}
