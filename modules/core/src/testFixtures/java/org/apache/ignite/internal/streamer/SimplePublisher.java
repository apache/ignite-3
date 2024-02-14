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

package org.apache.ignite.internal.streamer;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.SubmissionPublisher;
import org.apache.ignite.table.DataStreamerItem;

/**
 * Simple publisher that wraps items into {@link DataStreamerItem}.
 *
 * @param <T> Type of the item.
 */
public class SimplePublisher<T> implements Flow.Publisher<DataStreamerItem<T>>, AutoCloseable {
    private final SubmissionPublisher<DataStreamerItem<T>> publisher = new SubmissionPublisher<>();

    @Override
    public void subscribe(Subscriber<? super DataStreamerItem<T>> subscriber) {
        publisher.subscribe(subscriber);
    }

    public void submit(T item) {
        publisher.submit(DataStreamerItem.of((item)));
    }

    @Override
    public void close() {
        publisher.close();
    }
}
