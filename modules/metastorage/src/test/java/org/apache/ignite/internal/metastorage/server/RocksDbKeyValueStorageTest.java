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

package org.apache.ignite.internal.metastorage.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for RocksDB key-value storage implementation.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbKeyValueStorageTest extends BasicOperationsKeyValueStorageTest {
    @WorkDirectory
    private Path workDir;

    /** {@inheritDoc} */
    @Override
    KeyValueStorage createStorage() {
        return new RocksDbKeyValueStorage("test", workDir.resolve("storage"));
    }

    @Test
    void testWatchReplayOnSnapshotLoad() throws Exception {
        storage.put("foo".getBytes(UTF_8), "bar".getBytes(UTF_8), HybridTimestamp.MIN_VALUE);
        storage.put("baz".getBytes(UTF_8), "quux".getBytes(UTF_8), HybridTimestamp.MIN_VALUE);

        long revisionBeforeSnapshot = storage.revision();

        Path snapshotPath = workDir.resolve("snapshot");

        assertThat(storage.snapshot(snapshotPath), willCompleteSuccessfully());

        storage.close();

        storage = createStorage();

        storage.start();

        var latch = new CountDownLatch(2);

        storage.watchExact("foo".getBytes(UTF_8), 1, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(UTF_8)));

                latch.countDown();

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
                fail();
            }
        });

        storage.watchExact("baz".getBytes(UTF_8), 1, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                assertThat(event.entryEvent().newEntry().value(), is("quux".getBytes(UTF_8)));

                latch.countDown();

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
                fail();
            }
        });

        storage.startWatches((event, ts) -> CompletableFuture.completedFuture(null));

        storage.restoreSnapshot(snapshotPath);

        assertThat(storage.revision(), is(revisionBeforeSnapshot));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
}
