/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
        rateLimiterProcessor.subscribe(new Flow.Subscriber<Integer>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
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
