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

package org.apache.ignite.internal.table.distributed.raft.snapshot.incoming;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaResponse;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;

/**
 * Snapshot copier implementation for partitions. Used to stream partition data from the leader to the local node.
 */
public class IncomingSnapshotCopier extends SnapshotCopier {
    /** Messages factory. */
    private static final TableMessagesFactory MSG_FACTORY = new TableMessagesFactory();

    /** {@link SnapshotStorage} instance for the partition. */
    private final PartitionSnapshotStorage snapshotStorage;

    /** Snapshot URI. */
    private final SnapshotUri snapshotUri;

    /** Rebalance thread-pool, used to write data into a storage. */
    //TODO Use external pool.
    private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

    /**
     * Snapshot meta read from the leader.
     *
     * @see SnapshotMetaRequest
     */
    private SnapshotMeta snapshotMeta;

    /**
     * Constructor.
     *
     * @param snapshotStorage Snapshot storage.
     * @param snapshotUri Snapshot URI.
     */
    public IncomingSnapshotCopier(PartitionSnapshotStorage snapshotStorage, SnapshotUri snapshotUri) {
        this.snapshotStorage = snapshotStorage;
        this.snapshotUri = snapshotUri;
    }

    @Override
    public void cancel() {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17262
        // Implement.
    }

    @Override
    public void join() throws InterruptedException {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17262
        // Implement proper join.
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Override
    public void start() {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17262
        // What if node can't be resolved?
        ClusterNode sourceNode = snapshotStorage.topologyService().getByConsistentId(snapshotUri.nodeName);

        MessagingService messagingService = snapshotStorage.outgoingSnapshotsManager().messagingService();

        threadPool.submit(() -> {
            //TODO https://issues.apache.org/jira/browse/IGNITE-17262
            // Following code is just an example of what I expect and shouldn't be considered a template.
            CompletableFuture<NetworkMessage> metaRequestFuture = messagingService.invoke(
                    sourceNode,
                    MSG_FACTORY.snapshotMetaRequest().id(snapshotUri.snapshotId).build(),
                    1000L
            );

            metaRequestFuture.whenComplete((networkMessage, throwable) -> {
                SnapshotMetaResponse metaResponse = (SnapshotMetaResponse) networkMessage;

                snapshotMeta = metaResponse.meta();
            });
        });
    }

    @Override
    public SnapshotReader getReader() {
        // This one's called when "join" is complete.
        return new IncomingSnapshotReader(snapshotMeta);
    }

    @Override
    public void close() throws IOException {
        threadPool.shutdownNow();
    }
}
