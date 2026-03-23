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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;

/**
 * Test execution of MINUS (EXCEPT) operator.
 */
public class MinusExecutionTest extends AbstractSetOpExecutionTest {
    /** {@inheritDoc} */
    @Override
    protected AbstractSetOpNode<Object[]> setOpNodeFactory(ExecutionContext<Object[]> ctx,
            AggregateType type, int columnCount, boolean all, int inputsCnt) {

        StructNativeType rowSchema;

        switch (type) {
            case MAP:
                rowSchema = NativeTypes.structBuilder()
                        // input columns
                        .addField("C1", NativeTypes.STRING, false)
                        .addField("C1", NativeTypes.INT32, false)
                        // counters
                        .addField("C1", NativeTypes.INT32, false)
                        .addField("C1", NativeTypes.INT32, false)
                        .build();
                break;
            case REDUCE:
            case SINGLE:
                rowSchema = NativeTypes.structBuilder()
                        .addField("C1", NativeTypes.STRING, false)
                        .addField("C1", NativeTypes.INT32, false)
                        .build();
                break;
            default:
                throw new IllegalArgumentException("Unexpected aggregate type: " + type);
        }

        return new MinusNode<>(ctx, columnCount, type, all, ctx.rowFactoryFactory().create(rowSchema));
    }

    /** {@inheritDoc} */
    @Override
    protected void checkSetOp(boolean single, boolean all) {
        List<Object[]> ds1 = Arrays.asList(
                row("Igor", 1),
                row("Roman", 1),
                row("Igor", 1),
                row("Roman", 2),
                row("Igor", 1),
                row("Igor", 1),
                row("Igor", 1),
                row("Igor", 2),
                row("Alexey", 2)
        );

        List<Object[]> ds2 = Arrays.asList(
                row("Igor", 1),
                row("Roman", 1),
                row("Igor", 1),
                row("Alexey", 1)
        );

        List<Object[]> ds3 = Arrays.asList(
                row("Igor", 1),
                row("Alexey", 1),
                row("Alexey", 2)
        );

        List<Object[]> expectedResult;

        if (all) {
            expectedResult = Arrays.asList(
                    row("Igor", 1),
                    row("Igor", 1),
                    row("Igor", 2),
                    row("Roman", 2)
            );
        } else {
            expectedResult = Arrays.asList(
                    row("Igor", 2),
                    row("Roman", 2)
            );
        }

        checkSetOp(single, all, Arrays.asList(ds1, ds2, ds3), expectedResult);
    }
}
