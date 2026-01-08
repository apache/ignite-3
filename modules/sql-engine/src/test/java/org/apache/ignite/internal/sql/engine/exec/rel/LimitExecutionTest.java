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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.junit.jupiter.api.Test;

/**
 * Test LimitNode execution.
 */
@SuppressWarnings({"ThrowableNotThrown", "ResultOfObjectAllocationIgnored", "resource"})
public class LimitExecutionTest extends AbstractExecutionTest<Object[]> {
    @Test
    void offsetOnlyCaseIsNotSupportedBySortNode() {
        assertThrowsWithCause(
                () -> new SortNode<>(executionContext(), LimitExecutionTest::compareArrays, 10, -1),
                AssertionError.class,
                "Offset-only case is not supported by Sort node"
        );
    }

    /** Tests correct results fetched with Limit node. */
    @Test
    public void testLimit() {
        int bufSize = IN_BUFFER_SIZE;

        checkLimit(0, 1);
        checkLimit(1, 0);
        checkLimit(1, 1);
        checkLimit(0, bufSize);
        checkLimit(0, bufSize - 1);
        checkLimit(0, bufSize + 1);
        checkLimit(bufSize, 0);
        checkLimit(bufSize, bufSize);
        checkLimit(bufSize, bufSize - 1);
        checkLimit(bufSize, bufSize + 1);
        checkLimit(bufSize - 1, 1);
        checkLimit(2000, 0);
        checkLimit(0, 3000);
        checkLimit(2000, 3000);
    }

    /** Tests Sort node can limit its output when fetch param is set. */
    @Test
    public void testSortLimit() {
        int bufSize = IN_BUFFER_SIZE;

        checkLimitSort(0, 1);
        checkLimitSort(1, 1);
        checkLimitSort(0, bufSize);
        checkLimitSort(bufSize, bufSize);
        checkLimitSort(bufSize - 1, 1);
        checkLimitSort(0, 3000);
        checkLimitSort(2000, 3000);
    }

    /**
     * Check limit sort.
     *
     * @param offset Rows offset.
     * @param fetch Fetch rows count (zero means unlimited).
     */
    private void checkLimitSort(int offset, int fetch) {
        assert offset >= 0;
        assert fetch >= 0;

        ExecutionContext<Object[]> ctx = executionContext();

        RootNode<Object[]> rootNode = new RootNode<>(ctx);

        SortNode<Object[]> sortNode = new SortNode<>(ctx, LimitExecutionTest::compareArrays, offset,
                fetch == 0 ? -1 : fetch);

        List<Object[]> data = IntStream.range(0, IN_BUFFER_SIZE + fetch + offset).boxed()
                .map(i -> new Object[] {i}).collect(Collectors.toList());
        Collections.shuffle(data);

        ScanNode<Object[]> srcNode = new ScanNode<>(ctx, data);

        rootNode.register(sortNode);

        sortNode.register(srcNode);

        for (int i = offset; i < offset + fetch; i++) {
            assertTrue(rootNode.hasNext());
            assertEquals(i, rootNode.next()[0]);
        }

        assertEquals(fetch == 0, rootNode.hasNext());
    }

    /**
     * Check correct result size fetched.
     *
     * @param offset Rows offset.
     * @param fetch Fetch rows count (zero means unlimited).
     */
    private void checkLimit(int offset, int fetch) {
        ExecutionContext<Object[]> ctx = executionContext(true);

        RootNode<Object[]> rootNode = new RootNode<>(ctx);
        LimitNode<Object[]> limitNode = new LimitNode<>(ctx, offset, fetch == 0 ? -1 : fetch);
        List<Object[]> data = IntStream.range(0, IN_BUFFER_SIZE + fetch + offset).boxed()
                .map(i -> new Object[] {i}).collect(Collectors.toList());

        ScanNode<Object[]> srcNode = new ScanNode<>(ctx, data);

        rootNode.register(limitNode);
        limitNode.register(srcNode);

        for (int i = offset; i < offset + fetch; i++) {
            assertTrue(rootNode.hasNext());
            assertEquals(i, rootNode.next()[0]);
        }

        assertEquals(fetch == 0, rootNode.hasNext());
    }

    /**
     * Check the passed value to belong to array type.
     *
     * @param val Value to check.
     * @return {@code True} if not null and array.
     */
    private static boolean isArray(Object val) {
        return val != null && val.getClass().isArray();
    }

    /**
     * Compare arrays.
     *
     * @param a1 Value 1.
     * @param a2 Value 2.
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
     */
    private static int compareArrays(Object[] a1, Object[] a2) {
        if (a1 == a2) {
            return 0;
        }

        int l = Math.min(a1.length, a2.length);

        for (int i = 0; i < l; i++) {
            if (a1[i] == null || a2[i] == null) {
                if (a1[i] != null || a2[i] != null) {
                    return a1[i] != null ? 1 : -1;
                }

                continue;
            }

            if (isArray(a1[i]) && isArray(a2[i])) {
                int res = compareArrays((Object[]) a1[i], (Object[]) a2[i]);

                if (res != 0) {
                    return res;
                }
            }

            return ((Comparable) a1[i]).compareTo(a2[i]);
        }

        return Integer.compare(a1.length, a2.length);
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
