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

package org.apache.ignite.migrationtools.persistence.utils.pubsub;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class RateLimiterProcessorTest {

    @Test
    void manualTest() throws InterruptedException {
        StreamerPublisher<Integer> publisher = new StreamerPublisher<>();
        RateLimiterProcessor<Integer> rateLimiterProcessor = new RateLimiterProcessor<>(2, TimeUnit.SECONDS, 4);

        publisher.subscribe(rateLimiterProcessor);
        rateLimiterProcessor.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Integer.MAX_VALUE);
            }

            @Override
            public void onNext(Integer item) {
                System.out.printf("%d: %d\n", System.currentTimeMillis() / 1000, item);
            }

            @Override
            public void onError(Throwable throwable) {
                fail("received on error");
            }

            @Override
            public void onComplete() {

            }
        });

        for (int i = 0; i < 100; i++) {
            publisher.offer(i);
        }

        publisher.close();
    }

}
