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

import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.MAP;
import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.REDUCE;
import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.SINGLE;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.junit.jupiter.api.Test;

/**
 * Abstract test for set operator (MINUS, INTERSECT) execution.
 */
public abstract class AbstractSetOpExecutionTest extends AbstractExecutionTest {
    @Test
    public void testSingle() {
        checkSetOp(true, false);
    }

    @Test
    public void testSingleAll() {
        checkSetOp(true, true);
    }

    @Test
    public void testMapReduce() {
        checkSetOp(false, false);
    }

    @Test
    public void testMapReduceAll() {
        checkSetOp(false, true);
    }

    @Test
    public void testSingleWithEmptySet() {
        ExecutionContext<Object[]> ctx = executionContext();

        List<Object[]> data = Arrays.asList(
                row("Igor", 1),
                row("Roman", 1)
        );

        // For single distribution set operations, node should not request rows from the next source if result after
        // the previous source is already empty.
        ScanNode<Object[]> scan1 = new ScanNode<>(ctx, data);
        ScanNode<Object[]> scan2 = new ScanNode<>(ctx, data);
        ScanNode<Object[]> scan3 = new ScanNode<>(ctx, Collections.emptyList());
        Node<Object[]> node4 = new AbstractNode<>(ctx) {
            @Override
            protected void rewindInternal() {
                // No-op.
            }

            @Override
            protected Downstream<Object[]> requestDownstream(int idx) {
                return null;
            }

            @Override
            public void request(int rowsCnt) {
                fail("Node should not be requested");
            }
        };

        List<Node<Object[]>> inputs = Arrays.asList(scan1, scan2, scan3, node4);

        AbstractSetOpNode<Object[]> setOpNode = setOpNodeFactory(ctx, SINGLE, false, inputs.size());
        setOpNode.register(inputs);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(setOpNode);

        assertFalse(root.hasNext());
    }

    protected abstract void checkSetOp(boolean single, boolean all);

    /**
     * CheckSetOp.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param single Single.
     * @param all    All.
     */
    protected void checkSetOp(boolean single, boolean all, List<List<Object[]>> dataSets, List<Object[]> expectedResult) {
        ExecutionContext<Object[]> ctx = executionContext();

        List<Node<Object[]>> inputs = dataSets.stream().map(ds -> new ScanNode<>(ctx, ds))
                .collect(Collectors.toList());

        AbstractSetOpNode<Object[]> setOpNode;

        if (single) {
            setOpNode = setOpNodeFactory(ctx, SINGLE, all, inputs.size());
        } else {
            setOpNode = setOpNodeFactory(ctx, MAP, all, inputs.size());
        }

        setOpNode.register(inputs);

        if (!single) {
            AbstractSetOpNode<Object[]> reduceNode = setOpNodeFactory(ctx, REDUCE, all, 1);

            reduceNode.register(Collections.singletonList(setOpNode));

            setOpNode = reduceNode;
        }

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(RelCollations.of(ImmutableIntList.of(0, 1)));

        // Create sort node on the top to check sorted results.
        SortNode<Object[]> sortNode = new SortNode<>(ctx, cmp);
        sortNode.register(setOpNode);

        RootNode<Object[]> root = new RootNode<>(ctx);
        root.register(sortNode);

        assertTrue(nullOrEmpty(expectedResult) || root.hasNext());

        for (Object[] row : expectedResult) {
            assertArrayEquals(row, root.next());
        }

        assertFalse(root.hasNext());
    }

    protected abstract AbstractSetOpNode<Object[]> setOpNodeFactory(ExecutionContext<Object[]> ctx,
            AggregateType type, boolean all, int inputsCnt);
}
