package org.apache.ignite.internal.placementdrive;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Placement driver manager.
 */
public class PlacementDriverManager implements IgniteComponent {
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();

    /**
     * @param metaStorageMgr Metastorage manager.
     */
    public PlacementDriverManager(MetaStorageManager metaStorageMgr) {
    }

    /** {@inheritDoc} */
    @Override
    public void start() {

    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }
}
