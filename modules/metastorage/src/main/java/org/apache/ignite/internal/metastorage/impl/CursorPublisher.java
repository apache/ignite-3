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
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Publisher that streams data from a remote Meta Storage cursor.
 *
 * @see CursorSubscription
 */
class CursorPublisher implements Publisher<Entry> {
    private static final IgniteUuidGenerator UUID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    private final MetaStorageServiceContext context;

    private final Function<IgniteUuid, WriteCommand> createCursorSupplier;

    private final AtomicBoolean subscriptionGuard = new AtomicBoolean();

    /**
     * Creates a new publisher instance.
     *
     * @param context Context.
     * @param createCursorSupplier Factory that creates a remote Meta Storage cursor (provided with a cursor ID).
     */
    CursorPublisher(MetaStorageServiceContext context, Function<IgniteUuid, WriteCommand> createCursorSupplier) {
        this.context = context;
        this.createCursorSupplier = createCursorSupplier;
    }

    @Override
    public void subscribe(Subscriber<? super Entry> subscriber) {
        if (!subscriptionGuard.compareAndSet(false, true)) {
            throw new IllegalArgumentException("This publisher supports only one subscriber");
        }

        if (!context.busyLock().enterBusy()) {
            subscriber.onError(new NodeStoppingException());

            return;
        }

        try {
            IgniteUuid cursorId = UUID_GENERATOR.randomUuid();

            WriteCommand createCursorCommand = createCursorSupplier.apply(cursorId);

            context.raftService().run(createCursorCommand)
                    .whenCompleteAsync((v, e) -> {
                        if (!context.busyLock().enterBusy()) {
                            subscriber.onError(new NodeStoppingException());

                            return;
                        }

                        try {
                            if (e == null) {
                                var subscription = new CursorSubscription(context, cursorId, subscriber);

                                subscriber.onSubscribe(subscription);
                            } else {
                                subscriber.onError(e);
                            }
                        } finally {
                            context.busyLock().leaveBusy();
                        }
                    }, context.executorService());
        } finally {
            context.busyLock().leaveBusy();
        }
    }
}
