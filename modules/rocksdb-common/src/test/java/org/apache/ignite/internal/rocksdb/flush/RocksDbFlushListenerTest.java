/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/** Tests for {@link RocksDbFlushListener} */
public class RocksDbFlushListenerTest extends BaseIgniteAbstractTest {
    @Test
    void testSyncWalBeforeFlush() throws RocksDBException {
        RocksDB mockDb = mock(RocksDB.class);
        RocksDbFlusher flusher = new RocksDbFlusher(
                new IgniteSpinBusyLock(),
                new ScheduledThreadPoolExecutor(20),
                Executors.newSingleThreadExecutor(),
                () -> 0,
                () -> {} // No-op.
        );

        RocksDbFlushListener listener = new RocksDbFlushListener(flusher);

        listener.onFlushBegin(mockDb, mock(FlushJobInfo.class));

        verify(mockDb, times(1)).syncWal();
    }
}
