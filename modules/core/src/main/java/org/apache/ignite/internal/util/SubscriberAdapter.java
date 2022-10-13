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

package org.apache.ignite.internal.util;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;

/**
 * Subscriber adapter class.
 *
 * @param <SourceT> Source type.
 * @param <TargetT> Target type.
 */
public class SubscriberAdapter<SourceT, TargetT> implements Subscriber<SourceT> {
    private final Subscriber<? super TargetT> delegate;
    private final Function<SourceT, TargetT> converter;

    SubscriberAdapter(Subscriber<? super TargetT> delegate, Function<SourceT, TargetT> converter) {
        this.delegate = delegate;
        this.converter = converter;
    }

    /** {@inheritDoc} */
    @Override
    public void onSubscribe(Subscription subscription) {
        delegate.onSubscribe(subscription);
    }

    /** {@inheritDoc} */
    @Override
    public void onNext(SourceT item) {
        delegate.onNext(converter.apply(item));
    }

    /** {@inheritDoc} */
    @Override
    public void onError(Throwable th) {
        delegate.onError(th);
    }

    /** {@inheritDoc} */
    @Override
    public void onComplete() {
        delegate.onComplete();
    }
}
