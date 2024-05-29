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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunction;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionInstance;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ScanNode}.
 */
public class ScanNodeExecutionTest extends AbstractExecutionTest<Object[]> {

    @Test
    public void testIterableSource() {
        ExecutionContext<Object[]> ctx = executionContext(true);

        List<Object[]> data = IntStream.range(0, 5)
                .mapToObj(i -> new Object[]{i})
                .collect(Collectors.toList());

        RootNode<Object[]> rootNode = new RootNode<>(ctx);
        ScanNode<Object[]> srcNode = new ScanNode<>(ctx, data);

        rootNode.register(srcNode);

        collectResults(rootNode, data);
    }

    @Test
    public void testTableFunctionSource()  {
        ExecutionContext<Object[]> ctx = executionContext(true);

        List<Object[]> data = IntStream.range(0, 5)
                .mapToObj(i -> new Object[]{i})
                .collect(Collectors.toList());

        TestFunctionInstance<Object[]> instance = new TestFunctionInstance<>(data.iterator());
        TestFunction<Object[]> testFunction = new TestFunction<>(instance);

        try (RootNode<Object[]> rootNode = new RootNode<>(ctx)) {
            ScanNode<Object[]> srcNode = new ScanNode<>(ctx, testFunction);

            rootNode.register(srcNode);

            collectResults(rootNode, data);

            assertEquals(1, instance.closeCounter.get());
        }
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }

    private static void collectResults(RootNode<Object[]> rootNode, List<Object[]> data) {
        List<Object[]> actual = new ArrayList<>();

        while (rootNode.hasNext()) {
            actual.add(rootNode.next());
        }

        assertEquals(
                actual.stream().map(Arrays::asList).collect(Collectors.toList()),
                data.stream().map(Arrays::asList).collect(Collectors.toList())
        );
    }

    private static class TestFunction<RowT> implements TableFunction<RowT> {

        private final TableFunctionInstance<RowT> instance;

        private TestFunction(TableFunctionInstance<RowT> instance) {
            this.instance = instance;
        }

        @Override
        public TableFunctionInstance<RowT> createInstance(ExecutionContext<RowT> ctx) {
            return instance;
        }
    }

    private static class TestFunctionInstance<RowT> implements TableFunctionInstance<RowT> {
        final Iterator<RowT> it;

        final AtomicInteger closeCounter = new AtomicInteger();

        private TestFunctionInstance(Iterator<RowT> it) {
            this.it = it;
        }

        @Override
        public void close() {
            closeCounter.incrementAndGet();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public RowT next() {
            return it.next();
        }
    }
}
