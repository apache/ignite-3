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

package org.apache.ignite.internal.deployunit.metastore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.apache.ignite.internal.metastorage.Entry;

/**
 * Implementation of {@link Subscriber} based on {@link Entry}.
 *
 * @param <R> Result value type.
 */
public class EntrySubscriber<R> implements Subscriber<Entry> {
    private final CompletableFuture<R> result;

    private final Accumulator<R> accumulator;

    /**
     * Constructor.
     *
     * @param result Result future.
     * @param accumulator Values accumulator.
     */
    public EntrySubscriber(CompletableFuture<R> result, Accumulator<R> accumulator) {
        this.result = result;
        this.accumulator = accumulator;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Entry item) {
        accumulator.accumulate(item);
    }

    @Override
    public void onError(Throwable throwable) {
        result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
        try {
            result.complete(accumulator.get());
        } catch (AccumulateException e) {
            result.completeExceptionally(e.getCause());
        }
    }
}
