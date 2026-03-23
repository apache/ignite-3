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

import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Hash join execution tests. */
public class HashJoinExecutionTest extends AbstractJoinExecutionTest {
    @Override
    JoinAlgo joinAlgo() {
        return JoinAlgo.HASH;
    }

    @Test
    public void testHashJoinRewind() {
        ExecutionContext<Object[]> ctx = executionContext();

        ScanNode<Object[]> persons = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", 5},
                new Object[]{3, "Alexey", 1}
        ));

        ScanNode<Object[]> deps = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        ));

        HashJoinNode<Object[]> join = createJoinNode(ctx, LEFT, null);
        join.register(asList(persons, deps));

        RootRewindable<Object[]> node = new RootRewindable<>(ctx);
        node.register(join);

        Object[][] rows = fetchRows(node);

        assertEquals(4, rows.length);

        Object[][] expected = {
                {0, "Igor", 1, 1, "Core"},
                {1, "Roman", 2, 2, "SQL"},
                {2, "Ivan", 5, null, null},
                {3, "Alexey", 1, 1, "Core"}
        };

        assert2DimArrayEquals(expected, rows);

        deps = new ScanNode<>(ctx, Collections.singleton(new Object[]{5, "QA"}));
        join.register(asList(persons, deps));
        node.rewind();

        Object[][] rowsAfterRewind = fetchRows(node);

        assertEquals(4, rowsAfterRewind.length);

        Object[][] expectedAfterRewind = {
                {0, "Igor", 1, null, null},
                {1, "Roman", 2, null, null},
                {2, "Ivan", 5, 5, "QA"},
                {3, "Alexey", 1, null, null}
        };

        assert2DimArrayEquals(expectedAfterRewind, rowsAfterRewind);
    }

    @Test
    void innerHashJoinWithPostFiltration() {
        Object[][] persons = {
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", 5},
                new Object[]{3, "Alexey", 1}
        };

        Object[][] deps = {
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        };

        Object[][] expected = {
                {0, "Igor", 1, 1, "Core"},
                {3, "Alexey", 1, 1, "Core"},
        };

        BiPredicate<Object[], Object[]> condition = (l, r) -> ((String) r[1]).length() > 3;

        validate(INNER, condition, Stream.of(persons)::iterator, Stream.of(deps)::iterator, expected);
    }

    @Test
    void leftHashJoinWithPostFiltration() {
        Object[][] persons = {
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", 5},
                new Object[]{3, "Alexey", 1}
        };

        Object[][] deps = {
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        };

        Object[][] expected = {
                {0, "Igor", 1, 1, "Core"},
                {1, "Roman", 2, null, null},
                {2, "Ivan", 5, null, null},
                {3, "Alexey", 1, 1, "Core"},
        };

        BiPredicate<Object[], Object[]> condition = (l, r) -> ((String) r[1]).length() > 3;

        validate(LEFT, condition, Stream.of(persons)::iterator, Stream.of(deps)::iterator, expected);
    }

    @Test
    void semiHashJoinWithPostFiltration() {
        Object[][] persons = {
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", 5},
                new Object[]{3, "Alexey", 1}
        };

        Object[][] deps = {
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        };

        Object[][] expected = {
                new Object[]{0, "Igor", 1},
                new Object[]{3, "Alexey", 1}
        };

        BiPredicate<Object[], Object[]> condition = (l, r) -> ((String) r[1]).length() > 3;

        validate(SEMI, condition, Stream.of(persons)::iterator, Stream.of(deps)::iterator, expected);
    }

    private void validate(
            JoinRelType joinType,
            @Nullable BiPredicate<Object[], Object[]> condition,
            Iterable<Object[]> leftSource,
            Iterable<Object[]> rightSource,
            Object[][] expected
    ) {
        ExecutionContext<Object[]> ctx = executionContext();

        ScanNode<Object[]> left = new ScanNode<>(ctx, leftSource);
        ScanNode<Object[]> right = new ScanNode<>(ctx, rightSource);

        HashJoinNode<Object[]> join = createJoinNode(ctx, joinType, condition);

        join.register(asList(left, right));

        RootNode<Object[]> node = new RootNode<>(ctx);
        node.register(join);

        Object[][] rows = fetchRows(node);

        assert2DimArrayEquals(expected, rows);
    }

    private static Object[][] fetchRows(RootNode<Object[]> node) {
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(node, Spliterator.ORDERED), false)
                .toArray(Object[][]::new);
    }

    private static void assert2DimArrayEquals(Object[][] expected, Object[][] actual) {
        assertEquals(expected.length, actual.length, "expected length: " + expected.length + ", actual length: " + actual.length);

        Arrays.sort(actual, Comparator.comparing(r -> (int) r[0]));

        int length = expected.length;
        for (int i = 0; i < length; ++i) {
            Object[] exp = expected[i];
            Object[] act = actual[i];

            assertEquals(exp.length, act.length, "expected length: " + exp.length + ", actual length: " + act.length);
            assertArrayEquals(exp, act);
        }
    }
}
