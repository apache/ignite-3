package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;

public class NoneDeadlockPreventionUnboundedTest extends NoneDeadlockPreventionTest {
    @Override
    protected LockManager lockManager() {
        return new HeapUnboundedLockManager(deadlockPreventionPolicy());
    }
}
