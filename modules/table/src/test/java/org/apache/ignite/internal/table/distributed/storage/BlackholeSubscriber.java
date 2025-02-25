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

package org.apache.ignite.internal.table.distributed.storage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.apache.ignite.internal.schema.BinaryRow;

/**
 * Subscriber that requests everything the publisher has, discards any element it yields, completes the given future normally on
 * {@link Subscriber#onComplete()} and fails the future on {@link Subscriber#onError(Throwable)}.
 */
public class BlackholeSubscriber implements Subscriber<BinaryRow> {
    private final CompletableFuture<Void> resultFuture;

    public BlackholeSubscriber(CompletableFuture<Void> resultFuture) {
        this.resultFuture = resultFuture;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(BinaryRow item) {
        // No-op.
    }

    @Override
    public void onError(Throwable throwable) {
        resultFuture.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
        resultFuture.complete(null);
    }
}
