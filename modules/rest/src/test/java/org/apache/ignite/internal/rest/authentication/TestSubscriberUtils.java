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

package org.apache.ignite.internal.rest.authentication;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/** Implementation of {@link Subscriber} for tests. */
public class TestSubscriberUtils {

    /**
     * Subscribes to the given publisher, extracting a single value from it.
     *
     * @return Future that completes with the first value that was produced by this publisher, or fails with
     *     {@link NoSuchElementException}, if the publisher did not produce any items.
     */
    public static <T> CompletableFuture<T> subscribeToValue(Publisher<T> publisher) {
        var resultFuture = new CompletableFuture<T>();

        publisher.subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                subscription.request(1);
            }

            @Override
            public void onNext(T item) {
                resultFuture.complete(item);

                subscription.cancel();
            }

            @Override
            public void onError(Throwable throwable) {
                resultFuture.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                resultFuture.completeExceptionally(new NoSuchElementException());
            }
        });

        return resultFuture;
    }
}
