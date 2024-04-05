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

import static org.apache.ignite.internal.util.CompletableFutures.allOf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.VacuumTxStatesCommand;

public class PersistentTxStateVacuumizer {
    private static final IgniteLogger LOG = Loggers.forClass(PersistentTxStateVacuumizer.class);

    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    private final Function<TablePartitionId, RaftGroupService> txStateStorageResolver;

    public PersistentTxStateVacuumizer(Function<TablePartitionId, RaftGroupService> txStateStorageResolver) {
        this.txStateStorageResolver = txStateStorageResolver;
    }

    public CompletableFuture<IgniteBiTuple<Set<UUID>, Set<UUID>>> vacuumPersistentTxStates(Map<TablePartitionId, Set<UUID>> txIds) {
        Set<UUID> successful = ConcurrentHashMap.newKeySet();
        Set<UUID> unsuccessful = ConcurrentHashMap.newKeySet();
        List<CompletableFuture<?>> futures = new ArrayList<>();

        txIds.forEach((commitPartitionId, ids) -> {
            RaftGroupService raftClient = txStateStorageResolver.apply(commitPartitionId);

            if (raftClient != null) {
                VacuumTxStatesCommand cmd = TX_MESSAGES_FACTORY.vacuumTxStatesCommand().txIds(ids).build();

                CompletableFuture<?> future = raftClient.run(cmd).whenComplete((v, e) -> {
                    if (e == null) {
                        successful.addAll(ids);
                    } else {
                        LOG.warn("Failed to vacuum tx states from the persistent storage.", e);

                        unsuccessful.addAll(ids);
                    }
                });

                futures.add(future);
            }
        });

        return allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(unused -> new IgniteBiTuple<>(successful, unsuccessful));
    }
}
