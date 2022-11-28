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

package org.apache.ignite.internal.pagememory.persistence;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletableFuture;

/**
 * Helper class for tracking the completion of partition processing.
 *
 * <p>At the start of partition processing, you need to call {@link #incrementPartitionProcessingCounter()}, at the end
 * {@link #decrementPartitionProcessingCounter()}. When all partition processing is completed, the {@link #future()} will be completed.
 *
 * <p>If the {@link #future()} is completed, then subsequent calls to {@link #incrementPartitionProcessingCounter()} and
 * {@link #decrementPartitionProcessingCounter()} on this instance do not make sense, since we will not be able to wait for the completion
 * of the partition processing (we will not be able to reset the current future), in order not to get into such a situation, we need to
 * delete the current instance at the completion of the current future, and this may require external synchronization.
 */
class PartitionProcessingCounter {
    private static final VarHandle COUNTER;

    static {
        try {
            COUNTER = MethodHandles.lookup().findVarHandle(PartitionProcessingCounter.class, "counter", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Partition processing counter must be greater than or equal to zero. */
    @SuppressWarnings("unused")
    private volatile int counter;

    /** Future that will be completed when the {@link #counter} is zero. */
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    /**
     * Atomically increments the partition processing counter.
     */
    void incrementPartitionProcessingCounter() {
        assert !future.isDone();

        int updatedValue = (int) COUNTER.getAndAdd(this, 1) + 1;

        assert updatedValue > 0 : updatedValue;
    }

    /**
     * Atomically decrements the partition processing counter.
     */
    void decrementPartitionProcessingCounter() {
        assert !future.isDone();

        int updatedValue = (int) COUNTER.getAndAdd(this, -1) - 1;

        assert updatedValue >= 0 : updatedValue;

        if (updatedValue == 0) {
            future.complete(null);
        }
    }

    /**
     * Returns a future that will be completed when all partition processing has finished.
     */
    CompletableFuture<Void> future() {
        return future;
    }
}
