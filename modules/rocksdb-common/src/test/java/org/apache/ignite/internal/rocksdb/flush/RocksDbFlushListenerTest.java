package org.apache.ignite.internal.rocksdb.flush;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.junit.jupiter.api.Test;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDbFlushListenerTest extends BaseIgniteAbstractTest {

    @Test
    void testSyncWalBeforeFlush() throws RocksDBException {
        RocksDbFlusher flusher = new RocksDbFlusher(
                new IgniteSpinBusyLock(),
                new ScheduledThreadPoolExecutor(20),
                Executors.newSingleThreadExecutor(),
                () -> 0,
                () -> {} // No-op.
        );

        RocksDbFlushListener listener = new RocksDbFlushListener(flusher);

        RocksDB mockDb = mock(RocksDB.class);

        listener.onFlushBegin(mockDb, mock(FlushJobInfo.class));

        verify(mockDb, times(1)).syncWal();
    }
}
