package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;

public class TimeoutDeadlockPreventionUnboundedTest extends TimeoutDeadlockPreventionTest {
    @Override
    protected LockManager lockManager() {
        return new HeapUnboundedLockManager(new WaitDieDeadlockPreventionPolicy());
    }
}
