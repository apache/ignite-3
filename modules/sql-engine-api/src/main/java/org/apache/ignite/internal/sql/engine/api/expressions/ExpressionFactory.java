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

import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.sql.ColumnType;

/**
 * Factory for creating and compiling expressions from string representations.
 *
 * <p>This factory provides methods to parse, validate, and compile string-based expressions into executable predicates and other
 * expression types. Expressions are evaluated against structured row data using an {@link EvaluationContext} created by this factory's
 * {@link #contextBuilder()} method.
 *
 * <h2>Expression Syntax and Normalization</h2>
 *
 * <p>Expressions follow SQL-like syntax with support for identifiers, operators, and functions. All identifiers are normalized to
 * uppercase unless enclosed in double quotes. This means {@code column_name}, {@code COLUMN_NAME}, and {@code CoLuMn_NaMe} are treated as
 * identical, while {@code "column_name"} preserves exact case.
 *
 * <h2>Namespace and Column References</h2>
 *
 * <p>Expressions operate within namespaces that correspond to input data sources. For single-row predicates, an implicit "INPUT" namespace
 * is created, allowing both simple and qualified column references (e.g., {@code col_name} or {@code input.col_name}). A scalar on the
 * other hand has no input, thus has no namespace to refer to within expression.
 *
 * <h2>Usage Pattern</h2>
 * <pre>{@code
 * // Create a predicate
 * IgnitePredicate predicate = factory.predicate("age > 18 AND status = 'ACTIVE'", rowType);
 *
 * // Build evaluation context
 * EvaluationContext<MyRow> context = factory.<MyRow>contextBuilder()
 *     .timeProvider(() -> System.currentTimeMillis())
 *     .rowAccessor(myRowAccessor)
 *     .build();
 *
 * // Evaluate predicate
 * boolean result = predicate.test(myRow, context);
 * }</pre>
 *
 * <h2>Error Handling</h2>
 *
 * <p>Expression creation may fail at two stages:<ul>
 * <li><strong>Parsing:</strong> If the expression string is syntactically invalid, an
 * {@link ExpressionParsingException} is thrown.</li>
 * <li><strong>Validation:</strong> If the expression is syntactically correct but semantically
 * invalid (e.g., type mismatches, undefined columns), an {@link ExpressionValidationException} is thrown.</li>
 * </ul>
 *
 * @see EvaluationContext
 * @see EvaluationContextBuilder
 * @see ExpressionParsingException
 * @see ExpressionValidationException
 */
public interface ExpressionFactory {
    /**
     * Returns predicate which accepts a single row as input.
     *
     * <p>This expression has single namespace with name "INPUT" created implicitly. To refer to any input column, a simple identifier may
     * be used as well as fully qualified one: <pre>{@code
     *     // Given
     *     StructNativeType inputRowType = NativeTypes.structBuilder()
     *             .addField("INT_COL", NativeTypes.INT32, false)
     *             .build();
     *
     *     // This is correct
     *     factory.predicate("int_col > 5", inputRowType);
     *
     *     // This is also correct
     *     factory.predicate("input.int_col > 5", inputRowType)
     * }</pre>
     *
     * <p>The expression provided is subject to normalization. This implies, all identifiers are normalized (i.e. converted to upper case)
     * unless they wrapped in double quotes. That is, {@code int_col > 5} and {@code INT_COL > 5} are equal expressions.
     *
     * @param expression String representation of expression to create.
     * @param inputRowType A type of the input row.
     * @return A predicate representing the provided expression.
     * @throws ExpressionParsingException If the expression string is syntactically invalid.
     * @throws ExpressionValidationException If the expression is syntactically correct but semantically invalid (e.g., type
     *         mismatches, undefined columns).
     * @see ExpressionParsingException
     * @see ExpressionValidationException
     */
    IgnitePredicate predicate(
            String expression,
            StructNativeType inputRowType
    ) throws ExpressionParsingException, ExpressionValidationException;

    /**
     * Returns scalar which evaluates to a single value.
     *
     * <p>This expression has no input and can return value of a simple type (not {@link StructNativeType}/{@link ColumnType#STRUCT});
     *
     * @param expression String representation of expression to create.
     * @param resultType An expected type of value returned as result of evaluation.
     * @return A scalar representing the provided expression.
     * @throws ExpressionParsingException If the expression string is syntactically invalid.
     * @throws ExpressionValidationException If the expression is syntactically correct but semantically invalid (e.g., type mismatches).
     * @see ExpressionParsingException
     * @see ExpressionValidationException
     */
    IgniteScalar scalar(
            String expression,
            NativeType resultType
    ) throws ExpressionParsingException, ExpressionValidationException;

    /** Returns builder to create {@link EvaluationContext context} which should be used with expressions returned by this factory. */
    <RowT> EvaluationContextBuilder<RowT> contextBuilder();
}
