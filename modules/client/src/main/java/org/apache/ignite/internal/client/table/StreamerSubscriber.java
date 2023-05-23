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

package org.apache.ignite.internal.client.table;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.apache.ignite.table.DataStreamerOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Data streamer subscriber.
 */
class StreamerSubscriber<T> implements Subscriber<T> {
    private final StreamerBatchSender<T> batchSender;

    private final DataStreamerOptions options;

    private final CompletableFuture<Void> completionFut = new CompletableFuture<>();

    private @Nullable Flow.Subscription subscription;

    StreamerSubscriber(StreamerBatchSender<T> batchSender, @Nullable DataStreamerOptions options) {
        assert batchSender != null;

        this.batchSender = batchSender;
        this.options = options == null ? new DataStreamerOptions() : null;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;

        subscription.request(options.batchSize());
    }

    @Override
    public void onNext(T item) {
        // TODO: Update per-node buffers.
        // TODO: Request more data once current batch is processed.
    }

    @Override
    public void onError(Throwable throwable) {
        close();
    }

    @Override
    public void onComplete() {
        close();
    }

    private void close() {
        var s = subscription;

        if (s != null) {
            s.cancel();
        }

        completionFut.complete(null);
    }

    CompletableFuture<Void> completionFuture() {
        return completionFut;
    }
}
