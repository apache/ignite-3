/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.ignite.raft.jraft.util;

import java.util.Queue;import java.util.concurrent.ArrayBlockingQueue;import org.apache.ignite.raft.jraft.util.Recyclers.DefaultHandle;
import org.apache.ignite.raft.jraft.util.Recyclers.Stack;
import org.apache.ignite.raft.jraft.util.RecyclersHandlerTwoQueue.StackTwoQueue;import org.jetbrains.annotations.Nullable;

public class RecyclersHandlerSharedQueueOnly implements RecyclersHandler {
    public static final RecyclersHandlerSharedQueueOnly INSTANCE = new RecyclersHandlerSharedQueueOnly();

    public static class StackOnlySharedQueueOnly<T> extends Stack<T> {
        private final Queue<DefaultHandle> queue;

        StackOnlySharedQueueOnly(Recyclers<T> parent, Thread thread, int maxCapacity) {
            super(parent, thread, maxCapacity);

            queue = new ArrayBlockingQueue<>(maxCapacity);
        }
    }

    @Override
    public String name() {
        return "shared queue only";
    }

    @Override
    public void recycle(Thread t, Stack<?> s, DefaultHandle h) {
        ((StackOnlySharedQueueOnly<?>)s).queue.offer(h);
    }

    @Override
    public @Nullable DefaultHandle stackPop(Stack<?> s0) {
        StackOnlySharedQueueOnly<?> s = (StackOnlySharedQueueOnly<?>)s0;

        return s.queue.isEmpty() ? null : s.queue.poll();
    }

    @Override
    public <T> Stack<T> newStack(Recyclers<T> parent, Thread thread, int maxCapacity) {
        return new StackOnlySharedQueueOnly<>(parent, thread, maxCapacity);
    }
}
