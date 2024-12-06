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

package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.Constants.KiB;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.IgniteUtils.readableSize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.junit.jupiter.api.Test;

/**
 * Testing the node join speed when the meta storage snapshot size is greater than 100 mega-bytes and there are more than 100 thousand
 * records in the raft log.
 */
public class ItMetaStorageJoinNodeWithBigMetaStorageDataTest extends ClusterPerTestIntegrationTest {
    private static final byte[] VALUE = new byte[KiB];

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void test() throws Throwable {
        stopNode(1);

        MetaStorageManager metaStorageManager0 = igniteImpl(0).metaStorageManager();

        runWithTimed("loadMetaStorageByMemory", () -> loadMetaStorageByMemory(metaStorageManager0, 100));
        runWithTimed("doSnapshotOnMetaStorageRaftNode", this::doSnapshotOnMetaStorageRaftNode);
        runWithTimed("loadMetaStorageForBigRaftLog", () -> loadMetaStorageForBigRaftLog(metaStorageManager0, 100_000));

        runWithTimed("returnNode", () -> startNode(1));
    }

    private static void loadMetaStorageByMemory(MetaStorageManager metaStorageManager, int mibCount) {
        int entries = mibCount * KiB;

        var batch = new HashMap<ByteArray, byte[]>();

        for (int i = 0; i < entries; i++) {
            batch.put(ByteArray.fromString("keyMib=" + i), VALUE);

            if (i % 1_000 == 0) {
                assertThat(metaStorageManager.putAll(batch), willCompleteSuccessfully());
                batch.clear();
            }
        }

        assertThat(metaStorageManager.putAll(batch), willCompleteSuccessfully());
        batch.clear();

        LOG.info(
                ">>>>> Updated MetaStorage by memory finished: [entries={}, estimatedTotalValueSize={}]",
                entries, readableSize((long) mibCount * MiB, false)
        );
    }

    private static void loadMetaStorageForBigRaftLog(MetaStorageManager metaStorageManager, int entryCount) {
        int batchSize = 1_000;

        var futures = new ArrayList<CompletableFuture<Void>>(batchSize);

        for (int i = 0; i < entryCount; i++) {
            futures.add(metaStorageManager.put(ByteArray.fromString("keyEntry=" + i), VALUE));

            if (i % batchSize == 0) {
                assertThat(allOf(futures), willCompleteSuccessfully());
                futures.clear();
            }

            if (i % (10 * batchSize) == 0) {
                LOG.info(
                        ">>>>> Updated MetaStorage for big raft log in progress: [entries={}, i={}, remaining={}]",
                        entryCount, i, (entryCount - (i + 1))
                );
            }
        }

        assertThat(allOf(futures), willCompleteSuccessfully());
        futures.clear();

        LOG.info(">>>>> Updated MetaStorage for big raft log finished: [entries={}]", entryCount);
    }

    private void doSnapshotOnMetaStorageRaftNode() throws InterruptedException {
        ReplicationGroupId groupId = MetastorageGroupId.INSTANCE;

        RaftGroupService raftGroupService = cluster.leaderServiceFor(groupId);

        var snapshotLatch = new CountDownLatch(1);
        var snapshotStatus = new AtomicReference<Status>();

        raftGroupService.getRaftNode().snapshot(status -> {
            snapshotStatus.set(status);
            snapshotLatch.countDown();
        });

        assertTrue(snapshotLatch.await(1, TimeUnit.MINUTES), "Snapshot was not finished in time");
        assertTrue(snapshotStatus.get().isOk(), "Snapshot failed: " + snapshotStatus.get());
    }

    private static void runWithTimed(String name, RunnableX runnableX) throws Throwable {
        long startNanos = System.nanoTime();

        Throwable throwable = null;

        try {
            runnableX.run();
        } catch (Throwable t) {
            throwable = t;
        }

        long duration0 = System.nanoTime() - startNanos;
        long duration1 = duration0;

        var sb = new StringBuilder();

        long h = TimeUnit.HOURS.convert(duration0, TimeUnit.NANOSECONDS);
        if (h > 0) {
            sb.append(h).append("h ");
            duration0 -= TimeUnit.NANOSECONDS.convert(h, TimeUnit.HOURS);
        }

        long m = TimeUnit.MINUTES.convert(duration0, TimeUnit.NANOSECONDS);
        if (m > 0) {
            sb.append(m).append("m ");
            duration0 -= TimeUnit.NANOSECONDS.convert(m, TimeUnit.MINUTES);
        }

        long s = TimeUnit.SECONDS.convert(duration0, TimeUnit.NANOSECONDS);
        if (s > 0) {
            sb.append(s).append("s ");
            duration0 -= TimeUnit.NANOSECONDS.convert(s, TimeUnit.SECONDS);
        }

        long ms = TimeUnit.MILLISECONDS.convert(duration0, TimeUnit.NANOSECONDS);
        if (ms > 0) {
            sb.append(ms).append("ms ");
            duration0 -= TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
        }

        long us = TimeUnit.MILLISECONDS.convert(duration0, TimeUnit.MICROSECONDS);
        if (us > 0) {
            sb.append(us).append("us ");
            duration0 -= TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MICROSECONDS);
        }

        long ns = TimeUnit.NANOSECONDS.convert(duration0, TimeUnit.NANOSECONDS);
        if (ns > 0) {
            sb.append(ns).append("ns ");
        }

        LOG.info(
                ">>>>> Finish operation: [name={}, time={}, totalMs={}, totalNs={}]",
                throwable,
                name, sb.toString().trim(), TimeUnit.MILLISECONDS.convert(duration1, TimeUnit.NANOSECONDS), duration1
        );

        if (throwable != null) {
            throw throwable;
        }
    }
}
