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

package org.apache.ignite.internal.thread;

import static java.util.Collections.unmodifiableSet;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.jetbrains.annotations.Nullable;

/**
 * This class adds some necessary plumbing on top of the {@link Thread} class. Specifically, it adds:
 * <ul>
 *      <li>Consistent naming of threads;</li>
 *      <li>Name of the ignite node this thread belongs to.</li>
 * </ul>
 * <b>Note</b>: this class is intended for internal use only.
 */
public class IgniteThread extends Thread implements ThreadAttributes {
    private final Set<ThreadOperation> allowedOperations;

    /**
     * Creates thread with given worker.
     *
     * @param worker Worker to create thread with.
     */
    public IgniteThread(IgniteWorker worker) {
        this(worker.igniteInstanceName(), worker.name(), worker);
    }

    /**
     * Creates ignite thread with given name for a given Ignite instance.
     *
     * @param nodeName   Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread (will be added to the node name to form final name).
     * @param r          Runnable to execute.
     * @param allowedOperations Operations which this thread allows to execute.
     */
    public IgniteThread(String nodeName, String threadName, Runnable r, ThreadOperation... allowedOperations) {
        this(prefixWithNodeName(nodeName, threadName), r, allowedOperations);
    }

    /**
     * Creates ignite thread with given name.
     *
     * @param finalName Name of thread.
     * @param r Runnable to execute.
     * @param allowedOperations Operations which this thread allows to execute.
     */
    IgniteThread(String finalName, Runnable r, ThreadOperation... allowedOperations) {
        super(r, finalName);

        Set<ThreadOperation> operations = EnumSet.noneOf(ThreadOperation.class);
        Collections.addAll(operations, allowedOperations);
        this.allowedOperations = unmodifiableSet(operations);
    }

    /**
     * Returns IgniteThread or {@code null} if current thread is not an instance of IgniteThread.
     *
     * @return IgniteThread or {@code null} if current thread is not an instance of IgniteThread.
     */
    @Nullable
    public static IgniteThread current() {
        Thread thread = Thread.currentThread();

        return thread.getClass() == IgniteThread.class || thread instanceof IgniteThread
            ? ((IgniteThread) thread) : null;
    }

    /**
     * Create prefix for thread name.
     */
    public static String threadPrefix(String nodeName, String threadName) {
        return prefixWithNodeName(nodeName, threadName) + "-";
    }

    private static String prefixWithNodeName(String nodeName, String threadName) {
        return "%" + nodeName + "%" + threadName;
    }

    @Override
    public String toString() {
        return S.toString(IgniteThread.class, this, "name", getName());
    }

    @Override
    public Set<ThreadOperation> allowedOperations() {
        return allowedOperations;
    }
}
