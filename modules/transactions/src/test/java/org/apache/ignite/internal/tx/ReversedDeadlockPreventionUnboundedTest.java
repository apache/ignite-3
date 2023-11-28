package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;

public class ReversedDeadlockPreventionUnboundedTest extends ReversedDeadlockPreventionTest {
    @Override
    protected LockManager lockManager() {
        return new HeapUnboundedLockManager(new WaitDieDeadlockPreventionPolicy());
    }
}
