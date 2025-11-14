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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.GroupingAccumulator;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code GROUPING} accumulator function.
 */
public class GroupingAccumulatorTest {

    @Test
    public void testFactory() {
        assertThrows(IllegalArgumentException.class,
                () -> GroupingAccumulator.newAccumulator(List.of()).get(),
                "GROUPING function must have at least one argument"
        );
        assertThrows(IllegalArgumentException.class,
                () -> GroupingAccumulator.newAccumulator(IntStream.range(0, 64).boxed().collect(Collectors.toList())).get(),
                "GROUPING function with more than 63 arguments is not supported"
        );

        assertNotNull(GroupingAccumulator.newAccumulator(IntStream.range(0, 63).boxed().collect(Collectors.toList())).get());
    }

    @Test
    public void emptyState() {
        assumeTrue(IgniteUtils.assertionsEnabled(), "the test requires that assertions be enabled");

        Accumulator acc = newCall(List.of(0));
        AccumulatorsState result = newState();

        assertThrows(AssertionError.class, () -> acc.end(newState(), result));

        assertNull(result.get());
    }

    @Test
    public void accumulatorIgnoresValues() {
        Accumulator acc = newCall(List.of(0));

        AccumulatorsState state = newState();

        assertEquals(IgniteTypeFactory.INSTANCE.createSqlType(SqlTypeName.BIGINT), acc.returnType(IgniteTypeFactory.INSTANCE));
        assertThrows(UnsupportedOperationException.class, () -> acc.add(state, new Object[]{1}));

        assertFalse(state.hasValue());
    }

    @Test
    public void callWithSingleArgument() {
        Accumulator acc = newCall(List.of(1));

        // Empty group
        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of()), result);

            assertTrue(result.hasValue());
            assertEquals(0L, result.get());
        }

        // Aggregate by single column
        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of(1)), result);

            assertTrue(result.hasValue());
            assertEquals(1L, result.get());
        }

        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of(0)), result);

            assertTrue(result.hasValue());
            assertEquals(0L, result.get());
        }

        // Aggregate by multiple columns
        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of(0, 1)), result);

            assertTrue(result.hasValue());
            assertEquals(1L, result.get());
        }
    }

    @Test
    public void callWithMultipleArguments() {
        Accumulator acc = newCall(List.of(3, 2));

        // Empty group
        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of()), result);

            assertTrue(result.hasValue());
            assertEquals(0L, result.get());
        }

        // Aggregate by single column
        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of(1)), result);

            assertTrue(result.hasValue());
            assertEquals(0L, result.get());
        }

        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of(2)), result);

            assertTrue(result.hasValue());
            assertEquals(1L, result.get());
        }

        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of(3)), result);

            assertTrue(result.hasValue());
            assertEquals(2L, result.get());
        }

        // Aggregate by multiple columns
        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of(1, 2)), result);

            assertTrue(result.hasValue());
            assertEquals(1L, result.get());
        }

        {
            AccumulatorsState result = newState();
            acc.end(newState(ImmutableBitSet.of(2, 3)), result);

            assertTrue(result.hasValue());
            assertEquals(3L, result.get());
        }
    }

    private Accumulator newCall(List<Integer> columns) {
        return GroupingAccumulator.newAccumulator(columns).get();
    }

    private static AccumulatorsState newState() {
        AccumulatorsState state = new AccumulatorsState(1);
        state.setIndex(0);
        return state;
    }

    private static AccumulatorsState newState(Object arg) {
        AccumulatorsState state = new AccumulatorsState(1);
        state.setIndex(0);
        state.set(arg);
        return state;
    }
}
