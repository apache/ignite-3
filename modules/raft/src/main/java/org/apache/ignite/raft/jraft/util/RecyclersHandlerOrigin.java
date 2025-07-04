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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.raft.jraft.util.Recyclers.DefaultHandle;
import org.apache.ignite.raft.jraft.util.Recyclers.Stack;
import org.apache.ignite.raft.jraft.util.Recyclers.WeakOrderQueue;
import org.jetbrains.annotations.Nullable;

public class RecyclersHandlerOrigin extends RecyclersHandler {
    public static final RecyclersHandlerOrigin INSTANCE = new RecyclersHandlerOrigin();

    public static class StackOrigin<T> extends Stack<T> {
        StackOrigin(Recyclers<T> parent, Thread thread, int maxCapacity) {
            super(parent, thread, maxCapacity);

            elements = new DefaultHandle[Math.min(Recyclers.INITIAL_CAPACITY, maxCapacity)];
        }
    }

    @Override
    public String name() {
        return "origin";
    }

    @Override
    public void recycle(Thread t, Stack<?> s, DefaultHandle h) {
        if (t == s.thread) {
            s.push(h);
            return;
        }

        // we don't want to have a ref to the queue as the value in our weak map
        // so we null it out; to ensure there are no races with restoring it later
        // we impose a memory ordering here (no-op on x86)
        Map<Stack<?>, WeakOrderQueue> delayedRecycled = Recyclers.delayedRecycled.get();
        WeakOrderQueue queue = delayedRecycled.get(s);
        if (queue == null) {
            delayedRecycled.put(s, queue = new WeakOrderQueue(s, t));

            Recyclers.WEAK_ORDER_QUEUES.computeIfAbsent(s, stack1 -> ConcurrentHashMap.newKeySet()).add(queue);
        }
        queue.add(h);
    }

    @Override
    public @Nullable DefaultHandle stackPop(Stack<?> s) {
        return s.pop();
    }

    @Override
    public <T> Stack<T> newStack(Recyclers<T> parent, Thread thread, int maxCapacity) {
        return new StackOrigin<>(parent, thread, maxCapacity);
    }}
