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

import java.util.Objects;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Function;

/**
 * Publisher adapter class.
 *
 * @param <SourceT> Source type.
 * @param <TargetT> Target type.
 */
public class PublisherAdapter<SourceT, TargetT> implements Publisher<TargetT> {
    private final Publisher<SourceT> delegate;
    private final Function<SourceT, TargetT> converter;

    public PublisherAdapter(Publisher<SourceT> delegate, Function<SourceT, TargetT> converter) {
        this.delegate = Objects.requireNonNull(delegate, "Publisher");
        this.converter = Objects.requireNonNull(converter, "Converter");
    }

    /** {@inheritDoc} */
    @Override
    public void subscribe(Subscriber<? super TargetT> subscriber) {
        delegate.subscribe(new SubscriberAdapter<SourceT, TargetT>(subscriber, converter));
    }
}
