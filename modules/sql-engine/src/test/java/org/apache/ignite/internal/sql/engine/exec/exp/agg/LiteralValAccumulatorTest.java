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

import java.util.function.Supplier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.LiteralVal;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code LITERAL_AGG(EXPR)}.
 */
public class LiteralValAccumulatorTest extends BaseIgniteAbstractTest {

    @Test
    public void test() {
        StatefulAccumulator accumulator = newCall();

        // Literal agg accepts the same value.
        accumulator.add("1");

        assertEquals("1", accumulator.end());
    }

    @Test
    public void empty() {
        StatefulAccumulator accumulator = newCall();

        assertEquals(null, accumulator.end());
    }

    private StatefulAccumulator newCall() {
        Supplier<Accumulator> supplier = LiteralVal.newAccumulator(Commons.typeFactory().createSqlType(SqlTypeName.VARCHAR));
        return new StatefulAccumulator(supplier);
    }
}
