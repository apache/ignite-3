package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;

public class ReversedDeadlockPreventionUnboundedTest extends ReversedDeadlockPreventionTest {
    @Override
    protected LockManager lockManager() {
        return new HeapUnboundedLockManager(deadlockPreventionPolicy());
    }
}
