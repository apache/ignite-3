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

import static org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage.IGNITE_DISABLE_SYNC_AND_WAL_FOR_CHECKSUM;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.Constants.KiB;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.IgniteUtils.readableSize;
import static org.apache.ignite.internal.util.IgniteUtils.runWithTimed;
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
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Testing the node join speed when the meta storage snapshot size is greater than 100 mega-bytes and there are more than 100 thousand
 * records in the raft log.
 */
public class ItMetaStorageJoinNodeWithBigMetaStorageDataTest extends ClusterPerTestIntegrationTest {
    private static final byte[] VALUE = new byte[KiB];

    @BeforeAll
    static void beforeAll() {
        System.clearProperty(IGNITE_DISABLE_SYNC_AND_WAL_FOR_CHECKSUM);
    }

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Override
    protected boolean startClusterImmediately() {
        return false;
    }

    @ParameterizedTest(name = "disableSyncAndWalForChecksum = {0}, snapshotBeforeRestartNode = {1}")
    @CsvSource({
            "true, true",
            "true, false",
            "false, true",
            "false, false"
    })
    void test(boolean disableSyncAndWalForChecksum, boolean snapshotBeforeRestartNode) throws Throwable {
        System.setProperty(IGNITE_DISABLE_SYNC_AND_WAL_FOR_CHECKSUM, Boolean.toString(disableSyncAndWalForChecksum));

        startCluster();
        stopNode(1);

        MetaStorageManager metaStorageManager0 = igniteImpl(0).metaStorageManager();

        runWithTimed("loadMetaStorageByMemory", () -> loadMetaStorageByMemory(metaStorageManager0, 100));
        runWithTimed("doSnapshotOnMetaStorageRaftNodeAfterLoadByMemory", this::doSnapshotOnMetaStorageRaftNode);
        runWithTimed("loadMetaStorageForBigRaftLog", () -> loadMetaStorageForBigRaftLog(metaStorageManager0, 100_000));

        if (snapshotBeforeRestartNode) {
            runWithTimed("doSnapshotOnMetaStorageRaftNodeBeforeRestartNode", this::doSnapshotOnMetaStorageRaftNode);
        }

        runWithTimed("restartNode", () -> startNode(1));
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
}
