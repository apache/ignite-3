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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link BoundedPriorityBlockingQueue}.
 */
public class BoundedPriorityBlockingQueueTest {
    private BoundedPriorityBlockingQueue<Object> queue;
    private int maxCapacity = 1;

    @BeforeEach
    public void setup() {
        maxCapacity = 1;
        queue = new BoundedPriorityBlockingQueue<>(() -> maxCapacity);
    }

    @Test
    public void addMoreThatMaxCapacity() {
        assertThat(queue.offer(1), is(true));

        Assertions.assertThrows(QueueOverflowException.class, () -> queue.offer(2));
    }

    @Test
    public void emptyQueuePollWithTimeout() throws InterruptedException {
        Object poll = queue.poll(500, TimeUnit.MILLISECONDS);

        assertThat(poll, nullValue());
    }

    @Test
    public void emptyQueuePollLockThread() throws InterruptedException {
        Integer i = 1;
        AtomicBoolean pollSuccess = new AtomicBoolean();
        Thread thread = new Thread(() -> {
            try {
                Object poll = queue.take();

                assertThat(poll, is(i));
                pollSuccess.set(true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        thread.start();

        assertThat(thread.isAlive(), is(true));
        assertThat(pollSuccess.get(), is(false));
        assertThat(queue.offer(i), is(true));

        thread.join();

        assertThat(thread.isAlive(), is(false));
        assertThat(pollSuccess.get(), is(true));
    }

    @Test
    public void dynamicQueueCapacityChange() {
        assertThat(queue.offer(1), is(true));

        Assertions.assertThrows(QueueOverflowException.class, () -> queue.offer(2));

        maxCapacity = 2;

        assertThat(queue.offer(2), is(true));
    }

    @Test
    public void multipleAddGet() {
        maxCapacity = 2;

        assertThat(queue.offer(2), is(true));
        assertThat(queue.offer(1), is(true));

        assertThat(queue.poll(), is(1));
        assertThat(queue.poll(), is(2));

        SimpleComparable first = new SimpleComparable(2);
        assertThat(queue.offer(first), is(true));
        SimpleComparable second = new SimpleComparable(1);
        assertThat(queue.offer(second), is(true));

        assertThat(queue.poll(), is(second));
        assertThat(queue.poll(), is(first));

        SimpleComparable one = new SimpleComparable(1);
        assertThat(queue.offer(one), is(true));
        SimpleComparable anotherOne = new SimpleComparable(1);
        assertThat(queue.offer(anotherOne), is(true));

        assertThat(queue.poll(), is(one));
        assertThat(queue.poll(), is(anotherOne));
    }

    @Test
    public void correctPriorityOrder() {
        maxCapacity = 3;

        assertThat(queue.offer(3), is(true));
        assertThat(queue.offer(1), is(true));
        assertThat(queue.offer(2), is(true));

        assertThat(queue.poll(), is(1));

        assertThat(queue.offer(4), is(true));

        assertThat(queue.poll(), is(2));
        assertThat(queue.poll(), is(3));
        assertThat(queue.poll(), is(4));
    }

    @Test
    public void customComparator() {
        maxCapacity = 2;

        BoundedPriorityBlockingQueue<Integer> queue = new BoundedPriorityBlockingQueue<>(
                () -> maxCapacity,
                Comparator.<Integer>reverseOrder()
        );

        assertThat(queue.offer(2), is(true));
        assertThat(queue.offer(1), is(true));

        assertThat(queue.poll(), is(2));
        assertThat(queue.poll(), is(1));
    }

    private static class SimpleComparable implements Comparable<SimpleComparable> {
        private final Integer integer;

        private SimpleComparable(Integer integer) {
            this.integer = integer;
        }

        @Override
        public int compareTo(BoundedPriorityBlockingQueueTest.SimpleComparable o) {
            return Integer.compare(integer, o.integer);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SimpleComparable that = (SimpleComparable) o;

            return integer.equals(that.integer);
        }

        @Override
        public int hashCode() {
            return integer.hashCode();
        }
    }
}
