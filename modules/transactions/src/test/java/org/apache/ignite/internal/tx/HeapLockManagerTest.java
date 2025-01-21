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

import static org.apache.ignite.internal.tx.impl.HeapLockManager.DEFAULT_SLOTS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link HeapLockManager}.
 */
public class HeapLockManagerTest extends AbstractLockManagerTest {
    @Override
    protected LockManager newInstance(SystemLocalConfiguration systemLocalConfiguration) {
        HeapLockManager lockManager = new HeapLockManager(systemLocalConfiguration);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }

    @Override
    protected LockKey lockKey() {
        return new LockKey(0, "test");
    }

    @Test
    public void testDefaultConfiguration() {
        assertThat(((HeapLockManager) lockManager).available(), is(DEFAULT_SLOTS));
        assertThat(((HeapLockManager) lockManager).getSlots(), is(arrayWithSize(DEFAULT_SLOTS)));
    }

    @Test
    public void testNonDefaultConfiguration(
            @InjectConfiguration("mock.properties: { lockMapSize: \"42\", rawSlotsMaxSize: \"69\" }")
            SystemLocalConfiguration systemLocalConfiguration
    ) {
        var lockManager = new HeapLockManager(systemLocalConfiguration);

        lockManager.start(DeadlockPreventionPolicy.NO_OP);

        assertThat(lockManager.available(), is(42));
        assertThat(lockManager.getSlots(), is(arrayWithSize(69)));
    }
}
