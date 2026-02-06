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

package org.apache.ignite.internal.sql.engine.api.expressions;

/** Predicate expression which accepts single row as input and evaluates to boolean value. */
public interface IgnitePredicate {
    /**
     * Evaluates the predicate on the given row within the execution context.
     *
     * @param context The evaluation context, providing metadata for context-dependent functions, such as CURRENT_TIMESTAMP. Accepts
     *         only contexts created by the same {@link ExpressionFactory}.
     * @param row The current row to be evaluated.
     * @param <RowT> The type of the execution row.
     * @return {@code true} if the row satisfies the predicate, {@code false} otherwise.
     * @throws ExpressionEvaluationException If an error occurred during expression evaluation.
     * @see ExpressionFactory
     * @see ExpressionFactory#contextBuilder()
     */
    <RowT> boolean test(
            EvaluationContext<RowT> context,
            RowT row
    ) throws ExpressionEvaluationException;
}
