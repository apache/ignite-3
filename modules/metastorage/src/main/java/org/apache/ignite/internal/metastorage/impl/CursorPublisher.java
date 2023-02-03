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

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Function;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;

/**
 * Publisher that streams data from a remote Meta Storage cursor.
 *
 * @see CursorSubscription
 */
class CursorPublisher implements Publisher<Entry> {
    private static final IgniteUuidGenerator UUID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    private final RaftGroupService raftService;

    private final MetaStorageCommandsFactory commandsFactory;

    private final ExecutorService executorService;

    private final Function<IgniteUuid, WriteCommand> createCursorSupplier;

    /**
     * Creates a new publisher instance.
     *
     * @param raftService Meta Storage Raft service.
     * @param commandsFactory Meta Storage commands factory.
     * @param executorService Executor that will be used to call the subscriber's methods.
     * @param createCursorSupplier Factory that creates a remote Meta Storage cursor (provided with a cursor ID).
     */
    CursorPublisher(
            RaftGroupService raftService,
            MetaStorageCommandsFactory commandsFactory,
            ExecutorService executorService,
            Function<IgniteUuid, WriteCommand> createCursorSupplier
    ) {
        this.raftService = raftService;
        this.commandsFactory = commandsFactory;
        this.executorService = executorService;
        this.createCursorSupplier = createCursorSupplier;
    }

    @Override
    public void subscribe(Subscriber<? super Entry> subscriber) {
        IgniteUuid cursorId = UUID_GENERATOR.randomUuid();

        WriteCommand createCursorCommand = createCursorSupplier.apply(cursorId);

        raftService.run(createCursorCommand)
                .whenCompleteAsync((v, e) -> {
                    if (e == null) {
                        var subscription = new CursorSubscription(raftService, commandsFactory, executorService, cursorId, subscriber);

                        subscriber.onSubscribe(subscription);
                    } else {
                        subscriber.onError(e);
                    }
                }, executorService);
    }
}
