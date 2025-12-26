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

import static org.apache.ignite.internal.sql.engine.util.Commons.IN_BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.junit.jupiter.api.Test;

/**
 * Test FilterNode execution.
 */
public class FilterExecutionTest extends AbstractExecutionTest<Object[]> {
    @Test
    public void testFilter() {
        validateFilter(10, 50, r -> (int) r[0] >= 5 && (int) r[0] < 14);
        validateFilter(10, 50, r -> (int) r[0] >= 5 && (int) r[0] < 15);
        validateFilter(10, 50, r -> (int) r[0] >= 5 && (int) r[0] < 16);
    }

    @Test
    public void testFilterWithVariousBufferSize() {
        int bufSize = 1;
        validateFilter(bufSize, 0);
        validateFilter(bufSize, 1);
        validateFilter(bufSize, 10);

        bufSize = IN_BUFFER_SIZE;
        validateFilter(bufSize, 0);
        validateFilter(bufSize, bufSize - 1);
        validateFilter(bufSize, bufSize);
        validateFilter(bufSize, bufSize + 1);
        validateFilter(bufSize, 2 * bufSize);
    }

    private void validateFilter(int bufSize, int dataSize) {
        Stream<Object[]> stream = executeFilter(bufSize, dataSize, row -> true);

        assertEquals(dataSize, stream.count());
    }

    private void validateFilter(int bufSize, int dataSize, Predicate<Object[]> predicate) {
        Stream<Object[]> stream = executeFilter(bufSize, dataSize, predicate);

        int[] actual = stream.mapToInt(r -> (int) r[0]).toArray();
        int[] expected = IntStream.range(0, dataSize).mapToObj(i -> new Object[]{i}).filter(predicate).mapToInt(r -> (int) r[0]).toArray();

        assertArrayEquals(expected, actual);
    }

    private Stream<Object[]> executeFilter(int bufSize, int dataSize, Predicate<Object[]> predicate) {
        ExecutionContext<Object[]> ctx = executionContext(bufSize);

        ScanNode<Object[]> srcNode = new ScanNode<>(ctx, () -> IntStream.range(0, dataSize).mapToObj(i -> new Object[]{i}).iterator());
        FilterNode<Object[]> filterNode = new FilterNode<>(ctx, predicate);
        RootNode<Object[]> rootNode = new RootNode<>(ctx);

        filterNode.register(srcNode);
        rootNode.register(filterNode);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(rootNode, Spliterator.ORDERED), false);
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }

    @Override
    protected RowFactoryFactory<Object[]> rowFactoryFactory() {
        return ArrayRowHandler.INSTANCE;
    }
}
