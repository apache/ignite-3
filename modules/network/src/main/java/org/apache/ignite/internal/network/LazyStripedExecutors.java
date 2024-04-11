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

package org.apache.ignite.internal.network;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.thread.StripedExecutor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Lazy collection of striped executors indexed with short type. A striped executor is created on first execution with an index and remains
 * active forever (until this executor collection is closed).
 *
 * <p>After having been stopped, it never executes anything.
 */
abstract class LazyStripedExecutors implements ManuallyCloseable {
    private static final Executor NO_OP_EXECUTOR = task -> {};

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReferenceArray<StripedExecutor> array = new AtomicReferenceArray<>(Short.MAX_VALUE + 1);

    private final Object executorCreationMutex = new Object();

    /**
     * Executes a command on a stripe with the given index. If the executor is stopped, returns a special executor that executes nothing.
     *
     * @param executorIndex Index of the stripe.
     */
    public Executor executorFor(short executorIndex, int stripeIndex) {
        assert executorIndex >= 0 : "Executor index is negative: " + executorIndex;

        if (!busyLock.enterBusy()) {
            return NO_OP_EXECUTOR;
        }

        try {
            return stripedExecutorFor(executorIndex).stripeExecutor(stripeIndex);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private StripedExecutor stripedExecutorFor(short executorIndex) {
        StripedExecutor existing = array.get(executorIndex);

        if (existing != null) {
            return existing;
        }

        synchronized (executorCreationMutex) {
            existing = array.get(executorIndex);
            if (existing != null) {
                return existing;
            }

            StripedExecutor newExecutor = newStripedExecutor(executorIndex);

            array.set(executorIndex, newExecutor);

            return newExecutor;
        }
    }

    /**
     * Creates a new striped thread pool to serve an index.
     *
     * @param executorIndex Executor index for which the executor is being created.
     */
    protected abstract StripedExecutor newStripedExecutor(int executorIndex);

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        onStoppingInitiated();

        IntStream.range(0, array.length())
                .mapToObj(array::get)
                .filter(Objects::nonNull)
                .parallel()
                .forEach(executorService -> IgniteUtils.shutdownAndAwaitTermination(executorService, 10, SECONDS));
    }

    /**
     * Callback called just after the stop procedure forbade accepting new submissions (and hence creation of new executors).
     */
    protected void onStoppingInitiated() {
        // No-op.
    }
}
