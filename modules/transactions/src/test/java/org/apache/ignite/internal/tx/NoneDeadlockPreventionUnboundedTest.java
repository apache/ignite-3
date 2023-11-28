package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;

public class NoneDeadlockPreventionUnboundedTest extends NoneDeadlockPreventionTest {
    @Override
    protected LockManager lockManager() {
        return new HeapUnboundedLockManager(new WaitDieDeadlockPreventionPolicy());
    }
}
