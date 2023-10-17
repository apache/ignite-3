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

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.raft.ReadCommand;

/**
 * Publisher that streams data from a remote Meta Storage cursor.
 *
 * @see CursorSubscription
 */
class CursorPublisher implements Publisher<Entry> {
    private final MetaStorageServiceContext context;

    private final Function<byte[], ReadCommand> nextBatchCommandSupplier;

    private final AtomicBoolean subscriptionGuard = new AtomicBoolean();

    /**
     * Creates a new publisher instance.
     *
     * @param context Context.
     * @param nextBatchCommandSupplier Factory that creates a command for retrieving the next batch of values provided with the last
     *         processed key for pagination purposes.
     */
    CursorPublisher(MetaStorageServiceContext context, Function<byte[], ReadCommand> nextBatchCommandSupplier) {
        this.context = context;
        this.nextBatchCommandSupplier = nextBatchCommandSupplier;
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
            var subscription = new CursorSubscription(context, nextBatchCommandSupplier, subscriber);

            subscriber.onSubscribe(subscription);
        } finally {
            context.busyLock().leaveBusy();
        }
    }
}
