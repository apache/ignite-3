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

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.ignite.raft.jraft.util.Recyclers.DefaultHandle;
import org.apache.ignite.raft.jraft.util.Recyclers.Stack;
import org.jetbrains.annotations.Nullable;

public class RecyclersHandlerTwoQueue implements RecyclersHandler {
    public static final RecyclersHandlerTwoQueue INSTANCE = new RecyclersHandlerTwoQueue();

    public static class StackTwoQueue<T> extends Stack<T> {
        private final Queue<DefaultHandle> localQueue;

        private final Queue<DefaultHandle> sharedQueue;

        StackTwoQueue(Recyclers<T> parent, Thread thread, int maxCapacity) {
            super(parent, thread, maxCapacity);

            localQueue = new ArrayDeque<>(maxCapacity / 2);
            sharedQueue = new ArrayBlockingQueue<>(maxCapacity /2);
        }
    }

    @Override
    public String name() {
        return "two queue";
    }

    @Override
    public void recycle(Thread t, Stack<?> s, DefaultHandle h) {
        ((StackTwoQueue<?>)h.stackOrigin).sharedQueue.offer(h);
    }

    @Override
    public @Nullable DefaultHandle stackPop(Stack<?> s0) {
        StackTwoQueue<?> s = (StackTwoQueue<?>)s0;

        if (!s.localQueue.isEmpty()) {
            return s.localQueue.poll();
        } else if(!s.sharedQueue.isEmpty()) {
            Iterator<DefaultHandle> it = s.sharedQueue.iterator();

            while (it.hasNext() && s.localQueue.size() < s.maxCapacity / 2) {
                s.localQueue.add(it.next());

                it.remove();
            }

            return s.localQueue.poll();
        }

        return null;
    }

    @Override
    public <T> Stack<T> newStack(Recyclers<T> parent, Thread thread, int maxCapacity) {
        return new StackTwoQueue<>(parent, thread, maxCapacity);
    }
}
