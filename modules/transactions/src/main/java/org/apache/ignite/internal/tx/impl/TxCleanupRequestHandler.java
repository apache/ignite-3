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

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.internal.tx.impl.TxCleanupExceptionUtils.writeIntentSwitchFailureShouldBeLogged;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.message.CleanupReplicatedInfo;
import org.apache.ignite.internal.tx.message.CleanupReplicatedInfoMessage;
import org.apache.ignite.internal.tx.message.EnlistedPartitionGroupMessage;
import org.apache.ignite.internal.tx.message.TxCleanupMessage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicatedInfo;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Handles TX Cleanup request ({@link TxCleanupMessage}).
 */
public class TxCleanupRequestHandler {
    private static final IgniteLogger LOG = Loggers.forClass(TxCleanupRequestHandler.class);

    /** Tx messages factory. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Lock manager. */
    private final LockManager lockManager;

    private final ClockService clockService;

    private final Executor cleanupExecutor;

    /** Cleanup processor. */
    private final WriteIntentSwitchProcessor writeIntentSwitchProcessor;

    /** Cursor registry. */
    private final RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry;

    /** Volatile transaction state meta storage. */
    private final VolatileTxStateMetaStorage volatileTxStateMetaStorage;

    /** The map of txId to a cleanup context, tracking replicated write intents. */
    private final ConcurrentMap<UUID, CleanupContext> writeIntentsReplicated = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param messagingService Messaging service.
     * @param lockManager Lock manager.
     * @param clockService Clock service.
     * @param writeIntentSwitchProcessor A cleanup processor.
     * @param resourcesRegistry Resources registry.
     * @param cleanupExecutor Cleanup executor.
     * @param volatileTxStateMetaStorage Volatile transaction state meta storage.
     */
    public TxCleanupRequestHandler(
            MessagingService messagingService,
            LockManager lockManager,
            ClockService clockService,
            WriteIntentSwitchProcessor writeIntentSwitchProcessor,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            Executor cleanupExecutor,
            VolatileTxStateMetaStorage volatileTxStateMetaStorage
    ) {
        this.messagingService = messagingService;
        this.lockManager = lockManager;
        this.clockService = clockService;
        this.writeIntentSwitchProcessor = writeIntentSwitchProcessor;
        this.remotelyTriggeredResourceRegistry = resourcesRegistry;
        this.cleanupExecutor = cleanupExecutor;
        this.volatileTxStateMetaStorage = volatileTxStateMetaStorage;
    }

    /**
     * Starts the processor.
     */
    public void start() {
        messagingService.addMessageHandler(
                TxMessageGroup.class,
                (msg, sender, correlationId) -> {
                    if (msg instanceof TxCleanupMessage) {
                        cleanupExecutor.execute(() -> processTxCleanup((TxCleanupMessage) msg, sender, correlationId));
                    }
                }
        );
    }

    public void stop() {
    }

    private void processTxCleanup(TxCleanupMessage txCleanupMessage, InternalClusterNode sender, @Nullable Long correlationId) {
        assert correlationId != null;

        Map<EnlistedPartitionGroup, CompletableFuture<?>> writeIntentSwitches = new HashMap<>();

        // These cleanups will all be local.
        @Nullable List<EnlistedPartitionGroupMessage> partitionMessages = txCleanupMessage.groups();

        if (partitionMessages != null) {
            List<EnlistedPartitionGroup> partitions = asPartitionsList(partitionMessages);

            trackPartitions(
                    txCleanupMessage.txId(),
                    partitions.stream().map(EnlistedPartitionGroup::groupId).collect(Collectors.toSet()),
                    sender
            );

            for (EnlistedPartitionGroup partition : partitions) {
                CompletableFuture<Void> future = writeIntentSwitchProcessor.switchLocalWriteIntents(
                        partition,
                        txCleanupMessage.txId(),
                        txCleanupMessage.commit(),
                        txCleanupMessage.commitTimestamp()
                ).thenAccept(this::processWriteIntentSwitchResponse);

                writeIntentSwitches.put(partition, future);
            }
        }
        // First trigger the cleanup to properly release the locks if we know all affected partitions on this node.
        // If the partition collection is empty (likely to be the recovery case)- just run 'release locks'.
        allOf(writeIntentSwitches.values().toArray(new CompletableFuture<?>[0]))
                .whenComplete((unused, ex) -> {
                    releaseTxLocks(txCleanupMessage.txId());

                    remotelyTriggeredResourceRegistry.close(txCleanupMessage.txId());

                    NetworkMessage msg;
                    if (ex == null) {
                        msg = prepareResponse();
                    } else {
                        msg = prepareErrorResponse(txCleanupMessage.txId(), ex);

                        // Run durable cleanup for the partitions that we failed to cleanup properly.
                        // No need to wait on this future.
                        writeIntentSwitches.forEach((groupId, future) -> {
                            if (future.isCompletedExceptionally()) {
                                writeIntentSwitchProcessor
                                        .switchWriteIntentsWithRetry(
                                                txCleanupMessage.commit(),
                                                txCleanupMessage.commitTimestamp(),
                                                txCleanupMessage.txId(),
                                                groupId
                                        )
                                        .thenAccept(this::processWriteIntentSwitchResponse)
                                        .whenComplete((retryRes, retryEx) -> {
                                            if (retryEx != null && writeIntentSwitchFailureShouldBeLogged(retryEx)) {
                                                LOG.warn(
                                                        "Second cleanup attempt failed (the transaction outcome is not affected) {}.",
                                                        retryEx,
                                                        formatTxInfo(txCleanupMessage.txId(), volatileTxStateMetaStorage)
                                                );
                                            }
                                        });
                            }
                        });
                    }

                    messagingService.respond(sender, msg, correlationId);
                });
    }

    private void releaseTxLocks(UUID txId) {
        lockManager.releaseAll(txId);
    }

    private NetworkMessage prepareResponse() {
        return TX_MESSAGES_FACTORY
                .txCleanupMessageResponse()
                .timestamp(clockService.now())
                .build();
    }

    private NetworkMessage prepareResponse(CleanupReplicatedInfo result) {
        return TX_MESSAGES_FACTORY
                .txCleanupMessageResponse()
                .result(toCleanupReplicatedInfoMessage(result))
                .timestamp(clockService.now())
                .build();
    }

    private NetworkMessage prepareErrorResponse(UUID txId, Throwable th) {
        return TX_MESSAGES_FACTORY
                .txCleanupMessageErrorResponse()
                .txId(txId)
                .throwable(th)
                .timestamp(clockService.now())
                .build();
    }

    /**
     * Start tracking the cleanup replication process for the provided transaction.
     *
     * @param txId Transaction id.
     * @param groups Replication groups.
     * @param sender Cleanup request sender, needed to send cleanup replicated response.
     */
    private void trackPartitions(UUID txId, Set<ZonePartitionId> groups, InternalClusterNode sender) {
        writeIntentsReplicated.put(txId, new CleanupContext(sender, groups, groups));
    }

    /**
     * Process the replication response from a write intent switch result.
     *
     * @param result Write intent replication result.
     */
    private void processWriteIntentSwitchResponse(WriteIntentSwitchReplicatedInfo result) {
        if (result == null) {
            return;
        }

        writeIntentSwitchReplicated(result);
    }

    /**
     * Process the replication response from a write intent switch request.
     *
     * @param info Write intent replication info.
     */
    void writeIntentSwitchReplicated(WriteIntentSwitchReplicatedInfo info) {
        CleanupContext cleanupContext = writeIntentsReplicated.computeIfPresent(info.txId(), (uuid, context) -> {
            Set<ZonePartitionId> partitions = new HashSet<>(context.partitions);
            partitions.remove(info.partitionId());

            return new CleanupContext(context.sender, partitions, context.initialPartitions);
        });

        if (cleanupContext != null && cleanupContext.partitions.isEmpty()) {
            // Means all write intents have been replicated.
            sendCleanupReplicatedResponse(info.txId(), cleanupContext.sender, cleanupContext.initialPartitions);

            writeIntentsReplicated.remove(info.txId());
        }
    }

    /**
     * Send cleanup replicated response back to the sender (which is the commit partition primary).
     *
     * @param txId Transaction id.
     * @param sender Cleanup request sender.
     * @param partitions Partitions that we received replication confirmation for.
     */
    private void sendCleanupReplicatedResponse(UUID txId, InternalClusterNode sender, Collection<ZonePartitionId> partitions) {
        messagingService.send(sender, ChannelType.DEFAULT, prepareResponse(new CleanupReplicatedInfo(txId, partitions)));
    }

    /**
     * Discards local write intents.
     *
     * @param partitions Partitions.
     * @param txId The transaction id.
     *
     * @return The future.
     */
    CompletableFuture<Void> discardLocalWriteIntents(List<EnlistedPartitionGroup> partitions, UUID txId) {
        Map<EnlistedPartitionGroup, CompletableFuture<?>> writeIntentSwitches = IgniteUtils.newHashMap(partitions.size());

        for (EnlistedPartitionGroup partition : partitions) {
            CompletableFuture<?> future = writeIntentSwitchProcessor.switchLocalWriteIntents(
                    partition,
                    txId,
                    false,
                    null
            );

            writeIntentSwitches.put(partition, future);
        }

        releaseTxLocks(txId);

        remotelyTriggeredResourceRegistry.close(txId);

        // We don't care about replicating discarded write intents state, because it will be lazily resolved if needed.
        return allOf(writeIntentSwitches.values().toArray(new CompletableFuture<?>[0]));
    }

    private static class CleanupContext {
        private final InternalClusterNode sender;

        private final Set<ZonePartitionId> partitions;

        private final Set<ZonePartitionId> initialPartitions;

        public CleanupContext(InternalClusterNode sender, Set<ZonePartitionId> partitions, Set<ZonePartitionId> initialPartitions) {
            this.sender = sender;
            this.partitions = partitions;
            this.initialPartitions = initialPartitions;
        }
    }

    private static CleanupReplicatedInfoMessage toCleanupReplicatedInfoMessage(CleanupReplicatedInfo info) {
        Collection<ZonePartitionId> partitions = info.partitions();
        List<ZonePartitionIdMessage> partitionMessages = new ArrayList<>(partitions.size());

        for (ZonePartitionId partition : partitions) {
            partitionMessages.add(toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, partition));
        }

        return TX_MESSAGES_FACTORY.cleanupReplicatedInfoMessage()
                .txId(info.txId())
                .partitions(partitionMessages)
                .build();
    }

    private static List<EnlistedPartitionGroup> asPartitionsList(List<EnlistedPartitionGroupMessage> messages) {
        var list = new ArrayList<EnlistedPartitionGroup>(IgniteUtils.capacity(messages.size()));

        for (EnlistedPartitionGroupMessage message : messages) {
            list.add(message.asPartitionInfo());
        }

        return list;
    }
}
