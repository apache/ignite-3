package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;

public class TimeoutDeadlockPreventionUnboundedTest extends TimeoutDeadlockPreventionTest {
    @Override
    protected LockManager lockManager() {
        return new HeapUnboundedLockManager(deadlockPreventionPolicy());
    }
}
