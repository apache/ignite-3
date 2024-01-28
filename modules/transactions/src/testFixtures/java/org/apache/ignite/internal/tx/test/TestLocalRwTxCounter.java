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

package org.apache.ignite.internal.tx.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.LocalRwTxCounter;

/** Implementation for tests. */
public class TestLocalRwTxCounter implements LocalRwTxCounter {
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public void incrementRwTxCount(HybridTimestamp beginTs) {
        assertTrue(lock.isHeldByCurrentThread());
    }

    @Override
    public void decrementRwTxCount(HybridTimestamp beginTs) {
        assertTrue(lock.isHeldByCurrentThread());
    }

    @Override
    public <T> T inUpdateRwTxCountLock(Supplier<T> supplier) {
        lock.lock();

        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }
}
