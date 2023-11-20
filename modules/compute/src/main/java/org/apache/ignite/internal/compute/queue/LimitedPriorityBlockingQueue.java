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

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Implementation of {@link PriorityBlockingQueue} with max size limitation.
 *
 * @param <E> The type of elements held in this queue.
 */
public class LimitedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E> {
    private final Supplier<Integer> maxSize;

    /**
     * Constructor.
     *
     * @param maxSize Max queue size supplier.
     */
    public LimitedPriorityBlockingQueue(Supplier<Integer> maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Constructor.
     *
     * @param maxSize Max queue size supplier.
     * @param initialCapacity Initial queue capacity.
     */
    public LimitedPriorityBlockingQueue(Supplier<Integer> maxSize, int initialCapacity) {
        super(initialCapacity);
        this.maxSize = maxSize;
        checkInsert(initialCapacity);
    }

    /**
     * Constructor.
     *
     * @param maxSize Max queue size supplier.
     * @param initialCapacity Initial queue capacity.
     * @param comparator the comparator that will be used to order this priority queue.
     *     If {@code null}, the {@linkplain Comparable natural ordering} of the elements will be used.
     */
    public LimitedPriorityBlockingQueue(Supplier<Integer> maxSize, int initialCapacity, Comparator<? super E> comparator) {
        super(initialCapacity, comparator);
        this.maxSize = maxSize;
        checkInsert(initialCapacity);
    }

    @Override
    public boolean add(E o) {
        checkInsert(1);
        return super.add(o);
    }

    @Override
    public boolean offer(E o) {
        checkInsert(1);
        return super.offer(o);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) {
        checkInsert(1);
        return super.offer(e, timeout, unit);
    }

    @Override
    public void put(E o) {
        checkInsert(1);
        super.put(o);
    }

    @Override
    public int remainingCapacity() {
        return maxSize.get() - size();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        checkInsert(c.size());
        return super.addAll(c);
    }

    private void checkInsert(int size) {
        Integer maxSize = this.maxSize.get();
        int currentSize = size();
        if (currentSize > maxSize - size) {
            throw new QueueOverflowException("Compute queue overflow when tried to insert " + size + " element(s) to queue. "
                    + "Current queue size " + currentSize + ". "
                    + "Max queue size is " + maxSize + ".");
        }
    }

}
