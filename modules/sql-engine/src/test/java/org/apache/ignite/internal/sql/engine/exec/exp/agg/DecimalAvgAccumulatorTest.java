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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.DecimalAvg;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code AVG(DECIMAL)} accumulator function.
 */
public class DecimalAvgAccumulatorTest extends BaseIgniteAbstractTest {

    @Test
    public void testScale3() {
        StatefulAccumulator acc = newCall(2);

        acc.add(new BigDecimal("50"));
        acc.add(new BigDecimal("20"));
        acc.add(new Object[]{null});
        acc.add(new BigDecimal("30"));

        assertEquals(new BigDecimal("33.33"), acc.end());
    }

    @Test
    public void testScale0() {
        StatefulAccumulator acc = newCall(0);

        acc.add(new BigDecimal("50"));
        acc.add(new BigDecimal("20"));
        acc.add(new Object[]{null});
        acc.add(new BigDecimal("30"));

        assertEquals(new BigDecimal("33"), acc.end());
    }

    @Test
    public void empty() {
        StatefulAccumulator acc = newCall(3);

        assertNull(acc.end());
    }

    private StatefulAccumulator newCall(int scale) {
        return new StatefulAccumulator(DecimalAvg.FACTORY.apply(scale));
    }
}
