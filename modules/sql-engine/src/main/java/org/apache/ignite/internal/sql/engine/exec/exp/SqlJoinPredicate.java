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

import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;

/**
 * A functional interface representing a predicate specifically designed for SQL join operations.
 *
 * <p>This interface defines a single method, {@link #test(ExecutionContext, Object, Object)}, 
 * which evaluates a condition between two rows (the left and right rows) within the context of a join operation. 
 * It allows the predicate to be applied before the rows are fully joined, enabling efficient filtering 
 * of row pairs that do not satisfy the join condition.
 *
 * <p>By evaluating the predicate early, this interface can optimize join operations by avoiding 
 * unnecessary materialization of non-matching row pairs.
 */
@FunctionalInterface
public interface SqlJoinPredicate {
    /**
     * Evaluates the predicate between the left and right rows within the execution context.
     *
     * @param context The execution context, providing access to query-related data.
     * @param left The left row in the join operation.
     * @param right The right row in the join operation.
     * @param <RowT> The type of the execution row.
     * @return {@code true} if the pair of rows satisfies the predicate, {@code false} otherwise.
     */
    <RowT> boolean test(ExecutionContext<RowT> context, RowT left, RowT right);
}
