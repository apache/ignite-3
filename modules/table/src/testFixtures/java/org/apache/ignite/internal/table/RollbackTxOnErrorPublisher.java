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

package org.apache.ignite.internal.table;

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * A simple decorator that rollbacks the given transaction in case of any error supplied by original
 * publisher to the subscriber.
 *
 * @param <T> Type of the published items.
 */
public class RollbackTxOnErrorPublisher<T> implements Publisher<T> {
    private static class RollbackTxOnErrorSubscriber<T> implements Subscriber<T> {
        private final InternalTransaction tx;
        private final Subscriber<T> delegate;

        private RollbackTxOnErrorSubscriber(InternalTransaction tx, Subscriber<T> delegate) {
            this.tx = tx;
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override
        public void onSubscribe(Subscription subscription) {
            delegate.onSubscribe(subscription);
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(T item) {
            delegate.onNext(item);
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable throwable) {
            try {
                delegate.onError(throwable);
            } finally {
                tx.rollback();
            }
        }

        /** {@inheritDoc} */
        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }

    private final InternalTransaction tx;
    private final Publisher<T> delegate;

    /**
     * Constructs the object.
     *
     * @param tx A transaction to rollback in case of any error supplied to the subscriber.
     * @param delegate An original publisher.
     */
    public RollbackTxOnErrorPublisher(InternalTransaction tx, Publisher<T> delegate) {
        this.tx = tx;
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        delegate.subscribe(new RollbackTxOnErrorSubscriber<>(tx, subscriber));
    }
}
