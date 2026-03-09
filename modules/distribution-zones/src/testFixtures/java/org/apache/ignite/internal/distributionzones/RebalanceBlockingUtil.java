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

package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Statement;
import org.apache.ignite.internal.metastorage.dsl.Statement.UpdateStatement;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Util methods to block rebalance messages,
 * to be more precise these methods allows to block switching pending assignments to the stable ones.
 */
public class RebalanceBlockingUtil {
    /**
     * Drops network messages that are matched to the provided {@code predicate}.
     *
     * @param messageServices Message services.
     * @param predicate Predicate to be used to filter messages.
     */
    public static void blockMessages(Stream<MessagingService> messageServices, BiPredicate<String, NetworkMessage> predicate) {
        messageServices.forEach(messagingService -> {
            DefaultMessagingService defaultMessagingService = (DefaultMessagingService) messagingService;

            BiPredicate<String, NetworkMessage> oldPredicate = defaultMessagingService.dropMessagesPredicate();

            if (oldPredicate == null) {
                defaultMessagingService.dropMessages(predicate);
            } else {
                defaultMessagingService.dropMessages(oldPredicate.or(predicate));
            }
        });
    }

    /**
     * Stops blocking network messages.
     *
     * @param messageServices Message services.
     */
    public static void unblockMessages(Stream<MessagingService> messageServices) {
        messageServices.forEach(messagingService -> {
            DefaultMessagingService defaultMessagingService = (DefaultMessagingService) messagingService;

            defaultMessagingService.dropMessages((nodeName, message) -> false);
        });
    }

    /**
     * Blocks switching pending assignments key to a stable one.
     *
     * @param messageServices Message services.
     * @param replicationGroupId Replication group identifier to check.
     * @param blockedAssignments Assignments to be blocked.
     */
    public static void blockStableKeySwitch(
            Stream<MessagingService> messageServices,
            ZonePartitionId replicationGroupId,
            Assignments blockedAssignments
    ) {
        blockMessages(messageServices, KeySwitchPredicate.of(replicationGroupId, blockedAssignments));
    }

    /**
     * Blocks switching pending assignments key to a stable one.
     *
     * @param messageServices Message services.
     * @param replicationGroupId Replication group identifier to check.
     * @param blockedAssignments Assignments to be blocked.
     * @param initialAndPredicate Initial predicate to filter messages.
     */
    public static void blockStableKeySwitch(
            Stream<MessagingService> messageServices,
            ZonePartitionId replicationGroupId,
            Assignments blockedAssignments,
            BiPredicate<String, NetworkMessage> initialAndPredicate
    ) {
        blockMessages(messageServices, initialAndPredicate.and(KeySwitchPredicate.of(replicationGroupId, blockedAssignments)));
    }

    /**
     * Blocks switching pending assignments key to a stable one.
     *
     * @param messageServices Message services.
     * @param replicationGroupId Replication group identifier to check.
     * @param blockedAssignments Assignments to be blocked.
     * @param initialAndPredicate Initial predicate to filter messages.
     * @param reached Atomic boolean that will be set to {@code true} when at least one message is blocked.
     */
    public static void blockStableKeySwitch(
            Stream<MessagingService> messageServices,
            ZonePartitionId replicationGroupId,
            Assignments blockedAssignments,
            BiPredicate<String, NetworkMessage> initialAndPredicate,
            AtomicBoolean reached
    ) {
        blockMessages(messageServices, initialAndPredicate.and(KeySwitchPredicate.of(replicationGroupId, blockedAssignments, reached)));
    }

    /**
     * Blocks switching pending assignments key to a stable one for the given zone.
     *
     * @param messageServices Message services.
     * @param zoneId Zone identifier.
     * @param partitions Number of partitions.
     */
    public static void blockStableKeySwitch(
            Stream<MessagingService> messageServices,
            int zoneId,
            int partitions
    ) {
        if (partitions < 1) {
            throw new IllegalArgumentException("Invalid partitions number [partitions=" + partitions + ']');
        }

        BiPredicate<String, NetworkMessage> predicate = null;
        for (int partition = 0; partition < partitions; ++partition) {
            var partitionPredicate = new KeySwitchPredicate(new ZonePartitionId(zoneId, partition), (assignments) -> true, null);

            if (predicate == null) {
                predicate = partitionPredicate;
            } else {
                predicate = predicate.or(partitionPredicate);
            }
        }

        blockMessages(messageServices, predicate);
    }

    /**
     * Predicate to match assignments messages.
     */
    public static class KeySwitchPredicate implements BiPredicate<String, NetworkMessage> {
        @Nullable AtomicBoolean reached;

        final ZonePartitionId replicationGroupId;

        final Predicate<Assignments> assignmentsPredicate;

        private KeySwitchPredicate(
                ZonePartitionId replicationGroupId,
                Predicate<Assignments> assignmentsPredicate,
                @Nullable AtomicBoolean reached
        ) {
            this.reached = reached;
            this.replicationGroupId = replicationGroupId;
            this.assignmentsPredicate = assignmentsPredicate;
        }

        public static KeySwitchPredicate of(ZonePartitionId replicationGroupId, Assignments blockedAssignments) {
            return new KeySwitchPredicate(replicationGroupId, blockedAssignments::equals, null);
        }

        public static KeySwitchPredicate of(ZonePartitionId replicationGroupId, Assignments blockedAssignments, AtomicBoolean reached) {
            return new KeySwitchPredicate(replicationGroupId, blockedAssignments::equals, reached);
        }

        @Override
        public boolean test(String nodeName, NetworkMessage networkMessage) {
            if (networkMessage instanceof WriteActionRequest) {
                var writeActionRequest = (WriteActionRequest) networkMessage;
                WriteCommand command = writeActionRequest.deserializedCommand();

                if (command instanceof MultiInvokeCommand) {
                    MultiInvokeCommand multiInvokeCommand = (MultiInvokeCommand) command;

                    Statement andThen = multiInvokeCommand.iif().andThen();

                    if (andThen instanceof UpdateStatement) {
                        UpdateStatement updateStatement = (UpdateStatement) andThen;
                        List<Operation> operations = updateStatement.update().operations();

                        ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(replicationGroupId);

                        for (Operation operation : operations) {
                            ByteBuffer operationKey = operation.key();
                            if (operationKey == null) {
                                continue;
                            }
                            ByteArray opKey = new ByteArray(toByteArray(operationKey));

                            if (operation.type() == OperationType.PUT && opKey.equals(stablePartAssignmentsKey)) {
                                boolean equals = assignmentsPredicate.test(Assignments.fromBytes(toByteArray(operation.value())));

                                if (reached != null && equals) {
                                    reached.set(true);
                                }

                                return equals;
                            }
                        }
                    }
                }
            }

            return false;
        }
    }
}
