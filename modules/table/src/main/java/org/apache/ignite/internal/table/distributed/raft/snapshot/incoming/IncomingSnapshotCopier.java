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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot copier implementation for partitions. Used to stream partition data from the leader to the local node.
 */
// TODO: IGNITE-17894 реализовать и добвить все что нужно можно смотреть на LocalSnapshotCopier
public class IncomingSnapshotCopier extends SnapshotCopier {
    private static final IgniteLogger LOG = Loggers.forClass(IncomingSnapshotCopier.class);

    private static final TableMessagesFactory MSG_FACTORY = new TableMessagesFactory();
    private final PartitionSnapshotStorage snapshotStorage;

    /** Snapshot URI. */
    private final SnapshotUri snapshotUri;

    private final IgniteSpinBusyLock cancelLock = new IgniteSpinBusyLock();

    @Nullable
    private volatile CompletableFuture<?> future;

    // TODO: IGNITE-17894 реализовать по нормальному и конкурентному

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
    public void start() {
        future = CompletableFuture.runAsync(this::startCopy, snapshotStorage.getIncomingSnapshotsExecutor());
    }

    @Override
    public void join() throws InterruptedException {
        CompletableFuture<?> fut = future;

        assert fut != null;

        try {
            fut.get();
        } catch (CancellationException e) {
            // Ignored.
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public void cancel() {
        cancelLock.block();

        CompletableFuture<?> fut = future;

        assert fut != null;

        if (!isOk()) {
            setError(RaftError.ECANCELED, "Copier has been cancelled");
        }

        fut.cancel(true);
    }

    @Override
    public void close() throws IOException {
        cancel();

        try {
            join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public SnapshotReader getReader() {
        // This one's called when "join" is complete.
        return new IncomingSnapshotReader(snapshotMeta);
    }

    private void startCopy() {
        // TODO: 20.10.2022 реализовать
        // TODO: 20.10.2022 шаги:
        // TODO: 20.10.2022 1) пересоздать партицию : уничтожить её и создать заново
        // TODO: 20.10.2022 2) что-то сделать с appliedIndex или вроде того
        // TODO: 20.10.2022 3) начать скачивать записи наверное пачкой
        // TODO: 20.10.2022 4) записывать записи в партицию наверное пачкой
        // TODO: 20.10.2022 5) что-то сделать с appliedIndex или вроде того
        // TODO: 20.10.2022 ****) на кажом шаге может что-то отъебывать надо как-то это хендлить

        //TODO https://issues.apache.org/jira/browse/IGNITE-17262
        // What if node can't be resolved?
        ClusterNode sourceNode = snapshotStorage.topologyService().getByConsistentId(snapshotUri.nodeName);

        MessagingService messagingService = snapshotStorage.outgoingSnapshotsManager().messagingService();

        /*
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
         */
    }
}
