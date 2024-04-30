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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.func.SystemRangeTableFunction;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunction;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionInstance;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistry;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistryImpl;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.junit.jupiter.api.Test;

/** Tests for {@link SystemRangeTableFunction}. */
public class SystemRangeTableFunctionTest extends AbstractExecutionTest<Object[]>{

    private final TableFunctionRegistry registry = new TableFunctionRegistryImpl();

    private final RexBuilder rexBuilder = Commons.rexBuilder();

    @Test
    public void testSystemRangeNoIncrement() throws Exception {
        RexCall call = (RexCall) rexBuilder.makeCall(IgniteSqlOperatorTable.SYSTEM_RANGE,
                rexBuilder.makeBigintLiteral(BigDecimal.ONE),
                rexBuilder.makeBigintLiteral(BigDecimal.TEN)
        );

        List<Object[]> expected = IntStream.rangeClosed(1, 10)
                .mapToObj(i -> new Object[]{i})
                .collect(Collectors.toList());

        checkFunction(call, expected);
    }

    @Test
    public void testSystemRangeWithIncrement() throws Exception {
        RexCall call = (RexCall) rexBuilder.makeCall(IgniteSqlOperatorTable.SYSTEM_RANGE,
                rexBuilder.makeBigintLiteral(BigDecimal.ONE),
                rexBuilder.makeBigintLiteral(BigDecimal.TEN),
                rexBuilder.makeBigintLiteral(BigDecimal.ONE.add(BigDecimal.ONE))
        );

        List<Object[]> expected = IntStream.rangeClosed(1, 10)
                .filter(i -> i % 2 == 1)
                .mapToObj(i -> new Object[]{i})
                .collect(Collectors.toList());

        checkFunction(call, expected);
    }

    @Test
    public void testSystemRangeReverse() throws Exception {
        RexCall call = (RexCall) rexBuilder.makeCall(IgniteSqlOperatorTable.SYSTEM_RANGE,
                rexBuilder.makeBigintLiteral(BigDecimal.TEN),
                rexBuilder.makeBigintLiteral(BigDecimal.ONE),
                rexBuilder.makeBigintLiteral(BigDecimal.ONE.negate())
        );

        List<Object[]> expected = IntStream.rangeClosed(1, 10)
                .boxed()
                .sorted(Comparator.reverseOrder())
                .map(i -> new Object[]{i})
                .collect(Collectors.toList());

        checkFunction(call, expected);
    }

    private void checkFunction(RexCall call, List<Object[]> expected) throws Exception {
        ExecutionContext<Object[]> executionContext = executionContext();
        TableFunction<Object[]> tableFunction = registry.getTableFunction(executionContext, call);

        try (TableFunctionInstance<Object[]> instance = tableFunction.createInstance(executionContext)) {
            List<Object[]> actual = new ArrayList<>();
            while (instance.hasNext()) {
                actual.add(instance.next());
            }

            assertEquals(expected.size(), actual.size());
        }
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
