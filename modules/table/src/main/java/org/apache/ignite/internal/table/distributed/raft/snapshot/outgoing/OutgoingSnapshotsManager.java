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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotRequestMessage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataRequest;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Outgoing snapshots manager. Manages a collection of all ougoing snapshots, currently present on the Ignite node.
 */
public class OutgoingSnapshotsManager implements IgniteComponent {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(OutgoingSnapshotsManager.class);

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Map with outgoing snapshots. */
    private final ConcurrentMap<UUID, OutgoingSnapshot> outgoingSnapshots = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param messagingService Messaging service.
     */
    public OutgoingSnapshotsManager(MessagingService messagingService) {
        this.messagingService = messagingService;
    }

    /**
     * Returns a messaging service.
     */
    public MessagingService messagingService() {
        return messagingService;
    }

    @Override
    public void start() {
        messagingService.addMessageHandler(TableMessageGroup.class, this::messageHandler);
    }

    @Override
    public void stop() throws Exception {
    }

    /**
     * Registers an outgoing snapshot in the manager.
     *
     * @param snapshotId Snapshot id.
     * @param outgoingSnapshot Outgoing snapshot.
     */
    void registerOutgoingSnapshot(UUID snapshotId, OutgoingSnapshot outgoingSnapshot) {
        outgoingSnapshots.put(snapshotId, outgoingSnapshot);
    }

    /**
     * Removes an outgoing snapshot from the manager.
     *
     * @param snapshotId Snapshot id.
     */
    void finishOutgoingSnapshot(UUID snapshotId) {
        outgoingSnapshots.remove(snapshotId);
    }

    private void messageHandler(NetworkMessage networkMessage, NetworkAddress sender, @Nullable Long correlationId) {
        // Ignore all messages that we can't handle.
        if (!(networkMessage instanceof SnapshotRequestMessage)) {
            return;
        }

        assert correlationId != null;

        OutgoingSnapshot outgoingSnapshot = outgoingSnapshots.get(((SnapshotRequestMessage) networkMessage).id());

        if (outgoingSnapshot == null) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Unexpected snapshot request message has been received [message={}]", networkMessage);
            }

            return;
        }

        CompletableFuture<? extends NetworkMessage> responseFuture = handleSnapshotRequestMessage(networkMessage, outgoingSnapshot);

        if (responseFuture != null) {
            responseFuture.whenComplete((response, throwable) -> respond(response, throwable, sender, correlationId));
        }
    }

    private static @Nullable CompletableFuture<? extends NetworkMessage> handleSnapshotRequestMessage(
            NetworkMessage networkMessage,
            OutgoingSnapshot outgoingSnapshot
    ) {
        switch (networkMessage.messageType()) {
            case TableMessageGroup.SNAPSHOT_META_REQUEST:
                return outgoingSnapshot.handleSnapshotMetaRequest((SnapshotMetaRequest) networkMessage);

            case TableMessageGroup.SNAPSHOT_MV_DATA_REQUEST:
                return outgoingSnapshot.handleSnapshotMvDataRequest((SnapshotMvDataRequest) networkMessage);

            case TableMessageGroup.SNAPSHOT_TX_DATA_REQUEST:
                return outgoingSnapshot.handleSnapshotTxDataRequest((SnapshotTxDataRequest) networkMessage);

            default:
                return null;
        }
    }

    private CompletableFuture<Void> respond(
            NetworkMessage response,
            Throwable throwable,
            NetworkAddress sender,
            Long correlationId
    ) {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17262
        // Handle offline sender and stopped manager.
        return messagingService.respond(sender, response, correlationId);
    }
}
