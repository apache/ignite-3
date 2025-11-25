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

package org.apache.ignite.internal.table.distributed;

import java.util.concurrent.Executor;
import java.util.function.IntFunction;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.DataPresence;
import org.apache.ignite.internal.partition.replicator.network.message.HasDataRequest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.TableViewInternal;


class PartitionReplicatorNodeRecovery {
    private static final PartitionReplicationMessagesFactory TABLE_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

    private final MessagingService messagingService;

    private final Executor storageAccessExecutor;

    /** Obtains a TableImpl instance by a table ID. */
    private final IntFunction<TableViewInternal> tableById;

    PartitionReplicatorNodeRecovery(
            MessagingService messagingService,
            Executor storageAccessExecutor,
            IntFunction<TableViewInternal> tableById
    ) {
        this.messagingService = messagingService;
        this.storageAccessExecutor = storageAccessExecutor;
        this.tableById = tableById;
    }

    /**
     * Starts the component.
     */
    void start() {
        addMessageHandler();
    }

    private void addMessageHandler() {
        messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, (message, sender, correlationId) -> {
            if (message instanceof HasDataRequest) {
                // This message queries if a node has any data for a specific partition of a table
                assert correlationId != null;

                HasDataRequest msg = (HasDataRequest) message;

                storageAccessExecutor.execute(() -> handleHasDataRequest(msg, sender, correlationId));
            }
        });
    }

    private void handleHasDataRequest(HasDataRequest msg, InternalClusterNode sender, Long correlationId) {
        int tableId = msg.tableId();
        int partitionId = msg.partitionId();

        DataPresence dataPresence = DataPresence.UNKNOWN;

        TableViewInternal table = tableById.apply(tableId);

        if (table != null) {
            MvTableStorage storage = table.internalTable().storage();

            try {
                MvPartitionStorage mvPartition = storage.getMvPartition(partitionId);

                if (mvPartition != null) {

                    dataPresence = mvPartition.closestRowId(RowId.lowestRowId(partitionId)) != null
                            ? DataPresence.HAS_DATA : DataPresence.EMPTY;
                }
            } catch (StorageClosedException | StorageRebalanceException ignored) {
                // Ignoring so we'll return UNKNOWN for storageHasData meaning that we have no idea.
            }
        }

        messagingService.respond(
                sender,
                TABLE_MESSAGES_FACTORY.hasDataResponse().presenceString(dataPresence.name()).build(),
                correlationId
        );
    }
}
