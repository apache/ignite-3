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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PartitionOperationInFlightLimiterTest {
    private static final long MAX_MEMORY = Runtime.getRuntime().maxMemory();

    @Test
    void zeroHeapPercentAlwaysPermits() {
        var limiter = new PartitionOperationInflightLimiter(0);

        for (int i = 0; i < 100; i++) {
            assertTrue(limiter.tryAcquire(1000));
        }
    }

    @Test
    void negativeHeapPercentAlwaysPermits() {
        var limiter = new PartitionOperationInflightLimiter(-1);

        for (int i = 0; i < 100; i++) {
            assertTrue(limiter.tryAcquire(1000));
        }
    }

    @Test
    void acquireFailsWhenByteLimitExceeded() {
        // Use 10% heap limit.
        var limiter = new PartitionOperationInflightLimiter(10);
        long limit = (long) (0.10 * MAX_MEMORY);

        // A single chunk that exceeds the limit should be rejected.
        assertFalse(limiter.tryAcquire((int) Math.min(limit + 1, Integer.MAX_VALUE)));
    }

    @Test
    void acquireSucceedsUpToLimit() {
        var limiter = new PartitionOperationInflightLimiter(10);
        long limit = (long) (0.10 * MAX_MEMORY);

        // Chunk size that fits within the limit.
        int chunkBytes = (int) Math.min(limit / 2, Integer.MAX_VALUE / 2);

        assertTrue(limiter.tryAcquire(chunkBytes));
        assertTrue(limiter.tryAcquire(chunkBytes));
    }

    @Test
    void releaseRestoresBudget() {
        var limiter = new PartitionOperationInflightLimiter(10);
        long limit = (long) (0.10 * MAX_MEMORY);
        int chunkBytes = (int) Math.min(limit / 2, Integer.MAX_VALUE / 2);

        assertTrue(limiter.tryAcquire(chunkBytes));
        assertTrue(limiter.tryAcquire(chunkBytes));
        // Now at or near limit; another chunk should fail.
        assertFalse(limiter.tryAcquire(chunkBytes));

        limiter.release(chunkBytes);

        assertTrue(limiter.tryAcquire(chunkBytes));
    }

    @Test
    void releaseOnZeroLimitIsNoOp() {
        var limiter = new PartitionOperationInflightLimiter(0);

        // Should not throw.
        limiter.release(1000);

        assertTrue(limiter.tryAcquire(1000));
    }

    @Test
    void supplierConstructorInitializesLazily() {
        int[] callCount = {0};

        // 100% heap — effectively unlimited for this test.
        var limiter = new PartitionOperationInflightLimiter(() -> {
            callCount[0]++;
            return 100;
        });

        assertTrue(callCount[0] == 0, "supplier should not be called at construction time");

        assertTrue(limiter.tryAcquire(1));
        assertTrue(callCount[0] == 1, "supplier should be called exactly once");

        assertTrue(limiter.tryAcquire(1));
        assertTrue(callCount[0] == 1, "supplier should not be called again");
    }

    @Test
    void supplierConstructorWithZeroPercentAlwaysPermits() {
        var limiter = new PartitionOperationInflightLimiter(() -> 0);

        for (int i = 0; i < 100; i++) {
            assertTrue(limiter.tryAcquire(1000));
        }
    }

    @Test
    void multipleReleasesRestoreBudget() {
        var limiter = new PartitionOperationInflightLimiter(10);
        long limit = (long) (0.10 * MAX_MEMORY);
        int chunkBytes = (int) Math.min(limit / 4, Integer.MAX_VALUE / 4);

        // Acquire 4 chunks.
        for (int i = 0; i < 4; i++) {
            assertTrue(limiter.tryAcquire(chunkBytes), "acquire " + i + " should succeed");
        }

        // Release all.
        for (int i = 0; i < 4; i++) {
            limiter.release(chunkBytes);
        }

        // Should be able to acquire again.
        for (int i = 0; i < 4; i++) {
            assertTrue(limiter.tryAcquire(chunkBytes), "re-acquire " + i + " should succeed after release");
        }
    }
}
