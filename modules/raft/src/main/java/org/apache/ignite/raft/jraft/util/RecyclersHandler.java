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

import org.apache.ignite.raft.jraft.util.Recyclers.DefaultHandle;
import org.apache.ignite.raft.jraft.util.Recyclers.Stack;
import org.jetbrains.annotations.Nullable;

public interface RecyclersHandler {
    String name();

    void recycle(Thread t, Stack<?> s, DefaultHandle h);

    @Nullable DefaultHandle stackPop(Stack<?> s);

    <T> Stack<T> newStack(Recyclers<T> parent, Thread thread, int maxCapacity);
}
