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

package org.apache.ignite.internal.compute.queue;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for {@link PriorityQueueExecutor}.
 */
@ExtendWith(ConfigurationExtension.class)
public class PriorityQueueExecutorTest {
    private static final IgniteLogger LOG = Loggers.forClass(PriorityQueueExecutorTest.class);

    @InjectConfiguration
    private ComputeConfiguration configuration;

    private PriorityQueueExecutor priorityQueueExecutor;


    private void initExecutor(int threads) {
        if (priorityQueueExecutor != null) {
            priorityQueueExecutor.shutdown();
        }

        assertThat(configuration.change(computeChange -> computeChange.changeThreadPoolSize(threads)), willCompleteSuccessfully());

        priorityQueueExecutor = new PriorityQueueExecutor(
                configuration,
                new NamedThreadFactory(NamedThreadFactory.threadPrefix("testNode", "compute"), LOG)
        );
    }

    @Test
    public void testQueueIsWorking() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        CompletableFuture<Integer> task1 = priorityQueueExecutor.submit(() -> {
            latch1.await();
            return 0;
        });

        CompletableFuture<Integer> task2 = priorityQueueExecutor.submit(() -> {
            latch2.await();
            return 1;
        });

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));

        latch2.countDown();

        assertThat(task1, willTimeoutIn(100, TimeUnit.MILLISECONDS));
        assertThat(task2, willTimeoutIn(100, TimeUnit.MILLISECONDS));

        latch1.countDown();

        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2, willCompleteSuccessfully());
    }

    @Test
    public void testSubmitWithPriority() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);

        CompletableFuture<Integer> task1 = priorityQueueExecutor.submit(() -> {
            latch1.await();
            return 0;
        });

        CompletableFuture<Integer> task2 = priorityQueueExecutor.submit(() -> {
            latch2.await();
            return 1;
        }, 1);

        CompletableFuture<Integer> task3 = priorityQueueExecutor.submit(() -> {
            latch3.await();
            return 1;
        }, 2);


        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        latch1.countDown();

        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        //Current executing task is 3 because of priority.
        latch2.countDown();
        assertThat(task2, willTimeoutIn(100, TimeUnit.MILLISECONDS));
        assertThat(task3, willTimeoutIn(100, TimeUnit.MILLISECONDS));

        latch3.countDown();
        assertThat(task2, willCompleteSuccessfully());
        assertThat(task3, willCompleteSuccessfully());
    }

    @Test
    public void testSameOrder() {
        initExecutor(1);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);

        CompletableFuture<Integer> task1 = priorityQueueExecutor.submit(() -> {
            latch1.await();
            return 0;
        }, 1);

        CompletableFuture<Integer> task2 = priorityQueueExecutor.submit(() -> {
            latch2.await();
            return 1;
        }, 1);

        CompletableFuture<Integer> task3 = priorityQueueExecutor.submit(() -> {
            latch3.await();
            return 1;
        }, 1);

        assertThat(task1.isDone(), is(false));
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        latch1.countDown();
        assertThat(task1, willCompleteSuccessfully());
        assertThat(task2.isDone(), is(false));
        assertThat(task3.isDone(), is(false));

        //Current executing task is 3 because it was first.
        latch3.countDown();
        assertThat(task2, willTimeoutIn(100, TimeUnit.MILLISECONDS));
        assertThat(task3, willTimeoutIn(100, TimeUnit.MILLISECONDS));

        latch2.countDown();
        assertThat(task2, willCompleteSuccessfully());
        assertThat(task3, willCompleteSuccessfully());
    }
}
