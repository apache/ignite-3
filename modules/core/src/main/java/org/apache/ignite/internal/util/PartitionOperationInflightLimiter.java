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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.IgniteThrottledLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * Limits the total in-flight bytes of partition operations (queued or executing) across the replica manager and thin-client connector.
 *
 * <p>The byte limit is computed as a percentage of the JVM heap ({@code Runtime.getRuntime().maxMemory()}).
 * When the heap percentage is zero or less, all operations are permitted unconditionally.
 *
 * <p>{@link #tryAcquire(int)} returns {@code false} once adding {@code messageBytes} would exceed the limit.
 * A permit must be released via {@link #release(int)} when the operation completes.
 */
public class PartitionOperationInflightLimiter {

    /** Byte limit computed from heap percentage; {@code 0} means unlimited. */
    private volatile long byteLimit;

    private final @Nullable IntSupplier heapPercentSupplier;

    private volatile boolean initialized;

    /** Running total of in-flight bytes. */
    private final AtomicLong inFlightBytes = new AtomicLong();

    private final IgniteLogger log = Loggers.forClass(PartitionOperationInflightLimiter.class);

    private final IgniteThrottledLogger throttledLog = Loggers.toThrottledLogger(log);

    /**
     * Constructor.
     *
     * @param heapPercent Percentage of max JVM heap to use as the in-flight byte limit. Zero or negative disables the limit.
     */
    public PartitionOperationInflightLimiter(int heapPercent) {
        this.byteLimit = computeByteLimit(heapPercent);
        this.heapPercentSupplier = null;
        this.initialized = true;
    }

    /**
     * Constructor with a lazy supplier of the heap percentage.
     *
     * @param heapPercentSupplier Supplier of heap percentage (0 or less disables the limit). Called at most once, on first use.
     */
    public PartitionOperationInflightLimiter(@Nullable IntSupplier heapPercentSupplier) {
        this.heapPercentSupplier = heapPercentSupplier;
        this.initialized = false;
    }

    /**
     * Attempts to reserve {@code messageBytes} in-flight bytes.
     *
     * @param messageBytes Number of bytes to reserve.
     * @return {@code true} if the reservation was made or the limit is disabled; {@code false} if adding the bytes would exceed the limit.
     */
    public boolean tryAcquire(int messageBytes) {
        long limit = resolvedByteLimit();

        if (limit <= 0) {
            return true;
        }

        while (true) {
            long current = inFlightBytes.get();

            if (current + messageBytes > limit) {
                throttledLog.error("The node is overloaded, cannot permit partition operation requiring {} bytes", messageBytes);
                return false;
            }

            if (inFlightBytes.compareAndSet(current, current + messageBytes)) {
                return true;
            }
        }
    }

    /**
     * Releases previously reserved in-flight bytes.
     * Must only be called after a successful {@link #tryAcquire(int)}.
     *
     * @param messageBytes Number of bytes to release.
     */
    public void release(int messageBytes) {
        long limit = resolvedByteLimit();

        if (limit > 0) {
            inFlightBytes.addAndGet(-messageBytes);
        }
    }

    private long resolvedByteLimit() {
        if (initialized) {
            return byteLimit;
        }
        synchronized (this) {
            if (initialized) {
                return byteLimit;
            }
            if (heapPercentSupplier != null) {
                byteLimit = computeByteLimit(heapPercentSupplier.getAsInt());
            }
            initialized = true;
        }
        return byteLimit;
    }

    private static long computeByteLimit(int heapPercent) {
        if (heapPercent <= 0) {
            return 0;
        }
        return (long) (heapPercent / 100.0 * Runtime.getRuntime().maxMemory());
    }
}
