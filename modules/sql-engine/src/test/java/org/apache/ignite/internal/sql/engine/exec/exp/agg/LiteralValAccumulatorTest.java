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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.TimeZone;
import java.util.function.Supplier;
import org.apache.calcite.DataContext;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.LiteralVal;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.PlanUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code LITERAL_AGG(EXPR)}.
 */
public class LiteralValAccumulatorTest extends BaseIgniteAbstractTest {
    public static final DataContext DATA_CONTEXT = PlanUtils.defaultDataContext();

    @Test
    public void testBoolean() {
        Supplier<Accumulator> supplier = LiteralVal.newAccumulator(DATA_CONTEXT, Commons.rexBuilder().makeLiteral(true));
        StatefulAccumulator accumulator = new StatefulAccumulator(supplier);

        // Literal agg ignores its arguments.
        accumulator.add("1");

        assertEquals(true, accumulator.end());
    }

    @Test
    public void testTimestampWithLocalTimeZone() {
        long currentTimeMillis = System.currentTimeMillis();
        long adjustedTimeMillis = currentTimeMillis + TimeZone.getDefault().getOffset(currentTimeMillis);

        Supplier<Accumulator> supplier = LiteralVal.newAccumulator(
                DATA_CONTEXT,
                Commons.rexBuilder().makeTimestampWithLocalTimeZoneLiteral(TimestampString.fromMillisSinceEpoch(adjustedTimeMillis), 2)
        );

        StatefulAccumulator accumulator = new StatefulAccumulator(supplier);

        assertEquals(currentTimeMillis / 10 * 10, accumulator.end());
    }

    @Test
    public void testDecimal() {
        BigDecimal decimal = BigDecimal.valueOf(22.33d);
        Supplier<Accumulator> supplier = LiteralVal.newAccumulator(DATA_CONTEXT, Commons.rexBuilder().makeExactLiteral(decimal));

        StatefulAccumulator accumulator = new StatefulAccumulator(supplier);

        assertEquals(BigDecimal.valueOf(22.33d), accumulator.end());
    }

    @Test
    public void empty() {
        Supplier<Accumulator> supplier = LiteralVal.newAccumulator(DATA_CONTEXT, Commons.rexBuilder().makeLiteral(false));
        StatefulAccumulator accumulator = new StatefulAccumulator(supplier);

        assertEquals(false, accumulator.end());
    }
}
