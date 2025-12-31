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

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionEvaluationException;

/** Utility class providing helper methods for code generation. */
final class CodegenUtils {
    /**
     * Wraps a statement in a try-catch block that converts any thrown exceptions to {@link ExpressionEvaluationException}.
     *
     * <p>This method creates a try-catch wrapper around the provided statement. If any {@link Exception} is thrown during the execution of
     * the statement, it will be caught and re-thrown as an {@link ExpressionEvaluationException} with the original exception's message.
     *
     * @param statement The statement to be wrapped in exception handling logic.
     * @return A {@link BlockStatement} containing the wrapped statement with exception conversion logic.
     */
    static BlockStatement wrapWithConversionToEvaluationException(Statement statement) {
        ParameterExpression ex = Expressions.parameter(0, Exception.class, "e");
        Expression evaluationException = Expressions.new_(ExpressionEvaluationException.class, Expressions.call(ex, "getMessage"));
        BlockBuilder tryCatchBlock = new BlockBuilder();

        tryCatchBlock.add(Expressions.tryCatch(statement, Expressions.catch_(ex, Expressions.throw_(evaluationException))));

        return tryCatchBlock.toBlock();
    }

    private CodegenUtils() {
        throw new AssertionError("Should not be called");
    }
}
