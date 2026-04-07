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

package org.apache.ignite.internal.util;

import java.util.concurrent.Semaphore;
import java.util.function.IntSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * Limits the number of in-flight partition operations (queued or executing) across the replica manager and thin-client connector.
 *
 * <p>When the limit is zero or less, all operations are permitted unconditionally.
 * When positive, {@link #tryAcquire()} returns {@code false} once the limit is reached and the caller should reject the request.
 * A permit must be released via {@link #release()} upon operation completes.
 */
public class PartitionOperationInFlightLimiter {
    private volatile Semaphore semaphore;

    private final @Nullable IntSupplier limitSupplier;

    private volatile boolean initialized;

    /**
     * Constructor.
     *
     * @param maxInFlightPartitionOperationsPerCore Max number of in-flight partition operations per CPU core, or <= 0 to disable the limit.
     *     The total limit is {@code maxInFlightPartitionOperationsPerCore * availableProcessors}.
     */
    public PartitionOperationInFlightLimiter(int maxInFlightPartitionOperationsPerCore) {
        int limit = maxInFlightPartitionOperationsPerCore <= 0 ? 0
                : maxInFlightPartitionOperationsPerCore * Runtime.getRuntime().availableProcessors();
        this.semaphore = limit <= 0 ? null : new Semaphore(limit);
        this.limitSupplier = null;
        this.initialized = true;
    }

    /**
     * Constructor.
     *
     * @param maxInFlightPartitionOperationsPerCoreSupplier Supplier of the max number of in-flight partition operations per CPU core,
     *     or 0 to disable. The total limit is {@code supplied value * availableProcessors}.
     */
    public PartitionOperationInFlightLimiter(@Nullable IntSupplier maxInFlightPartitionOperationsPerCoreSupplier) {
        this.limitSupplier = maxInFlightPartitionOperationsPerCoreSupplier;
        this.initialized = false;
    }

    /**
     * Attempts to acquire a permit.
     *
     * @return {@code true} if a permit was acquired or the limit is disabled; {@code false} if the limit is reached.
     */
    public boolean tryAcquire() {
        Semaphore s = resolvedSemaphore();
        return s == null || s.tryAcquire();
    }

    /**
     * Releases a previously acquired permit.
     * Must only be called after a successful {@link #tryAcquire()} when the limit is enabled.
     */
    public void release() {
        Semaphore s = resolvedSemaphore();

        if (s != null) {
            s.release();
        }
    }

    private @Nullable Semaphore resolvedSemaphore() {
        if (initialized) {
            return semaphore;
        }
        synchronized (this) {
            if (initialized) {
                return semaphore;
            }
            if (limitSupplier != null) {
                int perCore = limitSupplier.getAsInt();
                int limit = perCore <= 0 ? 0 : perCore * Runtime.getRuntime().availableProcessors();

                if (limit > 0) {
                    this.semaphore = new Semaphore(limit);
                }
            }
            this.initialized = true;
        }
        return semaphore;
    }
}
