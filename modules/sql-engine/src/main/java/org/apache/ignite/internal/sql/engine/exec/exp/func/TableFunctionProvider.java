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

package org.apache.ignite.internal.sql.engine.exec.exp.func;

import java.util.function.Supplier;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;

/** Converts table functions to their implementations. */
public class TableFunctionProvider implements TableFunctionImplementor {

    /** {@inheritDoc} */
    @Override
    public <RowT> TableFunction<RowT> toTableFunction(ExecutionContext<RowT> ctx, RexCall rexCall) {
        if (rexCall.getOperator() == IgniteSqlOperatorTable.SYSTEM_RANGE) {
            Supplier<Long> start = implementExpr(ctx.expressionFactory(), rexCall.operands.get(0));
            Supplier<Long> end = implementExpr(ctx.expressionFactory(), rexCall.operands.get(1));
            Supplier<Long> increment;

            if (rexCall.operands.size() > 2) {
                increment = implementExpr(ctx.expressionFactory(), rexCall.operands.get(2));
            } else {
                increment = null;
            }

            return new SystemRangeFunction<>(start, end, increment);
        } else {
            throw new IllegalArgumentException("Unsupported table function: " + rexCall.getOperator());
        }
    }

    private static <RowT> Supplier<Long> implementExpr(ExpressionFactory<RowT> expressionFactory, RexNode expr) {
        if (expr == null) {
            return null;
        }

        Supplier<Object> value = expressionFactory.execute(expr);
        return () -> {
            Number num = (Number) value.get();
            if (num == null) {
                return null;
            }
            return num.longValue();
        };
    }
}
