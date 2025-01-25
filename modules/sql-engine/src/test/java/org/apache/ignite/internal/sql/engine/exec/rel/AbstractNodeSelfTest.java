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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.ExecutionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test for {@link Node} class.
 */
public class AbstractNodeSelfTest extends BaseIgniteAbstractTest {

    @Test
    public void testClose() {
        ExecutionContext<Object[]> ctx = Mockito.mock(ExecutionContext.class);

        when(ctx.executionId()).thenReturn(new ExecutionId(UUID.randomUUID(), 1));
        when(ctx.fragmentId()).thenReturn(1L);

        SimpleNode<Object[]> node = new SimpleNode<>(ctx);

        doAnswer(invocation -> {
            RunnableX runnableX = invocation.getArgument(0);
            runnableX.run();
            return null;
        }).when(ctx).execute(any(RunnableX.class), any(Consumer.class));

        // Schedule task 1
        node.execute(() -> node.request(1));
        assertEquals(1, node.callCount.get());

        // Schedule close
        node.execute(node::close);
        assertEquals(1, node.callCount.get());

        // Task is not called because the node was closed.
        node.execute(() -> node.request(1));
        assertEquals(1, node.callCount.get());

        // The last task was not scheduled
        verify(ctx, times(2)).execute(any(RunnableX.class), any(Consumer.class));
    }

    @Test
    public void testCloseDelayed() {
        ExecutionContext<Object[]> ctx = Mockito.mock(ExecutionContext.class);

        when(ctx.executionId()).thenReturn(new ExecutionId(UUID.randomUUID(), 1));
        when(ctx.fragmentId()).thenReturn(1L);

        SimpleNode<Object[]> node = new SimpleNode<>(ctx);

        doAnswer(invocation -> {
            RunnableX runnableX = invocation.getArgument(0);
            if (node.callCount.get() == 2) {
                // Add a call to close to check the following scenario:
                // Node:
                //   void someTask():
                //     this.execute(anotherTask)
                //     ...
                //     this.close()
                //
                // Because someTask first schedules anotherTask and then closes the node,
                // we do not want to execute anotherTask.
                node.close();
            }
            runnableX.run();
            return null;
        }).when(ctx).execute(any(RunnableX.class), any(Consumer.class));

        // Schedule task 1
        node.execute(() -> node.request(1));
        assertEquals(1, node.callCount.get());

        // Schedule task 2
        node.execute(() -> node.request(1));
        assertEquals(2, node.callCount.get());

        // Task is not called because the node was closed.
        node.execute(() -> node.request(1));
        assertEquals(2, node.callCount.get());

        // All tasks were scheduled
        verify(ctx, times(3)).execute(any(RunnableX.class), any(Consumer.class));
    }

    private static class SimpleNode<T> extends AbstractNode<T> implements Downstream<T> {

        private final AtomicInteger callCount = new AtomicInteger();

        /**
         * Constructor.
         *
         * @param ctx Execution context.
         */
        private SimpleNode(ExecutionContext<T> ctx) {
            super(ctx);
        }

        @Override
        protected void rewindInternal() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Downstream<T> requestDownstream(int idx) {
            return this;
        }

        @Override
        public void request(int rowsCnt) throws Exception {
            checkState();
            callCount.incrementAndGet();
        }

        @Override
        public void push(T row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void end() {
            throw new UnsupportedOperationException();
        }
    }
}
