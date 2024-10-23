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

package org.apache.ignite.internal.util.subscription;

import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper class for working with {@link Subscription}. Allows to safely manage subscriptions and their cancellation.
 */
class SubscriptionHolder {
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(null);

    public void setSubscription(Subscription subscription) {
        subscriptionRef.compareAndSet(null, subscription);
    }

    public void request(long n) {
        if (!isCancelled.get()) {
            Subscription subscription = subscriptionRef.get();
            if (subscription != null) {
                subscription.request(n);
            }
        }
    }

    public void cancel() {
        if (isCancelled.compareAndSet(false, true)) {
            subscriptionRef.get().cancel();
            subscriptionRef.set(null);
        }
    }

    public boolean isCancelled() {
        return isCancelled.get();
    }

    public boolean isNotCancelled() {
        return !isCancelled.get();
    }
}
