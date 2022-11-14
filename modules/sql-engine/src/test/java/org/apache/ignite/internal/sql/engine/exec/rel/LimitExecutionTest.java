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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.junit.jupiter.api.Test;

/**
 * Test LimitNode execution.
 */
public class LimitExecutionTest extends AbstractExecutionTest {
    /** Tests correct results fetched with Limit node. */
    @Test
    public void testLimit() {
        int bufSize = Commons.IN_BUFFER_SIZE;

        checkLimit(0, 1);
        checkLimit(1, 0);
        checkLimit(1, 1);
        checkLimit(0, bufSize);
        checkLimit(bufSize, 0);
        checkLimit(bufSize, bufSize);
        checkLimit(bufSize - 1, 1);
        checkLimit(2000, 0);
        checkLimit(0, 3000);
        checkLimit(2000, 3000);
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
        LimitNode<Object[]> limitNode = new LimitNode<>(ctx, () -> offset, fetch == 0 ? null : () -> fetch);
        SourceNode srcNode = new SourceNode(ctx);

        rootNode.register(limitNode);
        limitNode.register(srcNode);

        if (fetch > 0) {
            for (int i = offset; i < offset + fetch; i++) {
                assertTrue(rootNode.hasNext());
                assertEquals(i, rootNode.next()[0]);
            }

            assertFalse(rootNode.hasNext());
            assertEquals(srcNode.requested.get(), offset + fetch);
        } else {
            assertTrue(rootNode.hasNext());
            assertEquals(offset, rootNode.next()[0]);
            assertTrue(srcNode.requested.get() > offset);
        }
    }

    private static class SourceNode extends AbstractNode<Object[]> {

        AtomicInteger requested = new AtomicInteger();

        public SourceNode(ExecutionContext<Object[]> ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected Downstream<Object[]> requestDownstream(int idx) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void request(int rowsCnt) {
            int r = requested.getAndAdd(rowsCnt);

            context().execute(() -> {
                for (int i = 0; i < rowsCnt; i++) {
                    downstream().push(new Object[]{r + i});
                }
            }, this::onError);
        }
    }
}
