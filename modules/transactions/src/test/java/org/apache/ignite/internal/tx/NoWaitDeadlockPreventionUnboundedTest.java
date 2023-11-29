package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;

public class NoWaitDeadlockPreventionUnboundedTest extends NoWaitDeadlockPreventionTest {
    @Override
    protected LockManager lockManager() {
        return new HeapUnboundedLockManager(deadlockPreventionPolicy());
    }
}
