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

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Lazy striped executor indexed with short type. A thread is created on first execution with an index and remains active forever.
 *
 * <p>After having been stopped, it never executes anything.
 */
abstract class LazyStripedExecutor implements ManuallyCloseable {
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReferenceArray<ExecutorService> array = new AtomicReferenceArray<>(Short.MAX_VALUE + 1);

    /**
     * Executes a command on a stripe with the given index. If the executor is stopped, does nothing.
     *
     * @param index Index of the stripe.
     */
    public void execute(short index, Runnable command) {
        assert index >= 0 : "Index is negative: " + index;

        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            executorFor(index).execute(command);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private Executor executorFor(short index) {
        ExecutorService existing = array.get(index);

        if (existing != null) {
            return existing;
        }

        synchronized (array) {
            existing = array.get(index);
            if (existing != null) {
                return existing;
            }

            ExecutorService newExecutor = newSingleThreadExecutor(index);

            array.set(index, newExecutor);

            return newExecutor;
        }
    }

    /**
     * Creates a new single thread executor to serve a stripe.
     *
     * @param stripeIndex Stripe index for which the executor is being created.
     */
    protected abstract ExecutorService newSingleThreadExecutor(int stripeIndex);

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
                .forEach(executorService -> IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS));
    }

    /**
     * Callback called just after the stop procedure forbade accepting new submissions (and hence creation of new executors).
     */
    protected void onStoppingInitiated() {
        // No-op.
    }
}
