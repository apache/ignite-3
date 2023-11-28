package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;

public class NoWaitDeadlockPreventionUnboundedTest extends NoWaitDeadlockPreventionTest {
    @Override
    protected LockManager lockManager() {
        return new HeapUnboundedLockManager(new WaitDieDeadlockPreventionPolicy());
    }
}
