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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


/**
 * Unit tests for {@link TransformingPublisher}.
 */
@ExtendWith(MockitoExtension.class)
public class TransformingPublisherTest extends BaseIgniteAbstractTest {

    @Mock
    private Publisher<Integer> input;

    @Mock
    private Subscriber<String> subscriber;

    @Mock
    private Subscription subscription;

    /**
     * Successful test - terminates with onComplete.
     */
    @Test
    public void testOk() {
        ArgumentCaptor<Subscriber<Integer>> capture = ArgumentCaptor.forClass(Subscriber.class);

        doAnswer(invocation -> {
            // call onSubscribe in Publisher::subscribe
            Subscriber<Integer> s = invocation.getArgument(0);
            s.onSubscribe(subscription);
            return null;
        }).when(input).subscribe(capture.capture());

        TransformingPublisher<Integer, String> publisher = new TransformingPublisher<>(input, String::valueOf);
        publisher.subscribe(subscriber);

        Subscriber<Integer> innerSubscriber = capture.getValue();
        innerSubscriber.onNext(1);
        innerSubscriber.onNext(2);
        innerSubscriber.onComplete();

        verify(input, times(1)).subscribe(any(Subscriber.class));
        verify(subscriber, times(1)).onSubscribe(subscription);
        verify(subscriber, times(1)).onNext("1");
        verify(subscriber, times(1)).onNext("2");
        verify(subscriber, times(1)).onComplete();

        verifyNoMoreInteractions(input, subscriber, subscription);
    }

    /**
     * Unsuccessful case - terminates with onError.
     */
    @Test
    public void testError() {
        ArgumentCaptor<Subscriber<Integer>> capture = ArgumentCaptor.forClass(Subscriber.class);

        doAnswer(invocation -> {
            // call onSubscribe in Publisher::subscribe
            Subscriber<Integer> s = invocation.getArgument(0);
            s.onSubscribe(subscription);
            return null;
        }).when(input).subscribe(capture.capture());

        TransformingPublisher<Integer, String> publisher = new TransformingPublisher<>(input, String::valueOf);
        publisher.subscribe(subscriber);

        RuntimeException err = new RuntimeException();

        Subscriber<Integer> innerSubscriber = capture.getValue();
        innerSubscriber.onNext(1);
        innerSubscriber.onError(err);

        verify(input, times(1)).subscribe(any(Subscriber.class));
        verify(subscriber, times(1)).onSubscribe(subscription);
        verify(subscriber, times(1)).onNext("1");
        verify(subscriber, times(1)).onError(err);

        verifyNoMoreInteractions(input, subscriber, subscription);
    }
}
