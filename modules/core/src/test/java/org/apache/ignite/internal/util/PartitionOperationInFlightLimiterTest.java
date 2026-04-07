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
    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    @Test
    void zeroLimitAlwaysPermits() {
        var limiter = new PartitionOperationInFlightLimiter(0);

        for (int i = 0; i < 100; i++) {
            assertTrue(limiter.tryAcquire());
        }
    }

    @Test
    void totalLimitIsPerCoreTimesAvailableProcessors() {
        int perCore = 3;
        int expectedTotal = perCore * CPU_COUNT;
        var limiter = new PartitionOperationInFlightLimiter(perCore);

        for (int i = 0; i < expectedTotal; i++) {
            assertTrue(limiter.tryAcquire(), "acquire " + i + " should succeed");
        }
        assertFalse(limiter.tryAcquire(), "acquire beyond total limit should fail");
    }

    @Test
    void releaseRestoresPermit() {
        var limiter = new PartitionOperationInFlightLimiter(1);
        int total = CPU_COUNT;

        for (int i = 0; i < total; i++) {
            limiter.tryAcquire();
        }
        assertFalse(limiter.tryAcquire());

        limiter.release();

        assertTrue(limiter.tryAcquire());
    }

    @Test
    void releaseOnZeroLimitIsNoOp() {
        var limiter = new PartitionOperationInFlightLimiter(0);

        // Should not throw.
        limiter.release();

        assertTrue(limiter.tryAcquire());
    }

    @Test
    void supplierConstructorInitializesLazily() {
        int[] callCount = {0};

        var limiter = new PartitionOperationInFlightLimiter(() -> {
            callCount[0]++;
            return 1;
        });

        assertTrue(callCount[0] == 0, "supplier should not be called at construction time");

        int total = CPU_COUNT;
        for (int i = 0; i < total; i++) {
            assertTrue(limiter.tryAcquire());
        }
        assertFalse(limiter.tryAcquire());
        assertTrue(callCount[0] == 1, "supplier should be called exactly once");
    }

    @Test
    void supplierConstructorWithZeroLimitAlwaysPermits() {
        var limiter = new PartitionOperationInFlightLimiter(() -> 0);

        for (int i = 0; i < 100; i++) {
            assertTrue(limiter.tryAcquire());
        }
    }

    @Test
    void multipleReleasesRestoreMultiplePermits() {
        int perCore = 2;
        int total = perCore * CPU_COUNT;
        var limiter = new PartitionOperationInFlightLimiter(perCore);

        for (int i = 0; i < total; i++) {
            limiter.tryAcquire();
        }
        assertFalse(limiter.tryAcquire());

        for (int i = 0; i < total; i++) {
            limiter.release();
        }

        for (int i = 0; i < total; i++) {
            assertTrue(limiter.tryAcquire(), "re-acquire " + i + " should succeed after release");
        }
        assertFalse(limiter.tryAcquire());
    }
}
