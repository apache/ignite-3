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

package org.apache.ignite.internal.tx;

import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;

/**
 * Deadlock prevention test with modified prevention policy and transaction id generation algorithm, it is intended to test alternative
 * deadlock prevention policy.
 */
public class ReversedDeadlockPreventionTest extends AbstractDeadlockPreventionTest {
    private long counter;

    @BeforeEach
    public void before() {
        counter = 0;
    }

    @Override
    protected UUID beginTx() {
        counter++;
        return new UUID(0, Long.MAX_VALUE - counter);
    }

    @Override
    protected DeadlockPreventionPolicy deadlockPreventionPolicy() {
        return new DeadlockPreventionPolicy() {
            @Override
            public @Nullable Comparator<UUID> txComparator() {
                return Comparator.reverseOrder();
            }

            @Override
            public long waitTimeout() {
                return 0;
            }
        };
    }
}
