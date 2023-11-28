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

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Implementation of {@link PriorityBlockingQueue} with max size limitation.
 *
 * @param <E> The type of elements held in this queue.
 */
public class BoundedPriorityBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private final PriorityQueue<E> queue;

    private final Supplier<Integer> maxCapacity;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();

    /**
     * Constructor.
     *
     * @param maxCapacity Max queue size supplier.
     */
    public BoundedPriorityBlockingQueue(Supplier<Integer> maxCapacity) {
        queue = new PriorityQueue<>();
        this.maxCapacity = maxCapacity;
    }

    /**
     *  Creates a {@link BoundedPriorityBlockingQueue} with elements are ordered according to the specified comparator.
     *
     * @param maxCapacity Max queue size supplier.
     * @param comparator The comparator that will be used to order this
     *     priority queue. If {@code null}, the {@link Comparable} natural ordering of the elements will be used.
     */
    public BoundedPriorityBlockingQueue(Supplier<Integer> maxCapacity, Comparator<E> comparator) {
        queue = new PriorityQueue<>(comparator);
        this.maxCapacity = maxCapacity;
    }

    @Override
    public Iterator<E> iterator() {
        lock.lock();
        try {
            return new PriorityQueue<>(queue).iterator();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(E e) {
        lock.lock();
        try {
            checkInsert(1);
            boolean result = queue.offer(e);
            if (result) {
                notEmpty.signalAll();
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e, long l, TimeUnit timeUnit) {
        return offer(e);
    }

    @Override
    public E take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (queue.isEmpty()) {
                notEmpty.await();
            }
            E x = poll();
            assert x != null;
            return x;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll() {
        lock.lock();
        try {
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        long nanos = timeUnit.toNanos(l);
        lock.lockInterruptibly();
        try {
            E result = poll();
            while (result == null && nanos > 0) {
                nanos = notEmpty.awaitNanos(nanos);
                result = poll();
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        lock.lock();
        try {
            return maxCapacity.get() - queue.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> objects) {
        Objects.requireNonNull(objects);

        if (objects == this) {
            throw new IllegalArgumentException();
        }

        lock.lock();
        try {
            int j = 0; // num object added
            while (size() > 0) {
                objects.add(poll());
                j++;
            }
            return j;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> objects, int i) {
        Objects.requireNonNull(objects);

        if (objects == this) {
            throw new IllegalArgumentException();
        }
        lock.lock();
        try {
            int j = 0; // num object added
            while (size() > 0 && j < i) {
                objects.add(poll());
                j++;
            }
            return j;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E peek() {
        lock.lock();
        try {
            return queue.peek();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        lock.lock();
        try {
            return queue.contains(o);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        lock.lock();
        try {
            return queue.toArray(ts);
        } finally {
            lock.unlock();
        }
    }


    @Override
    public String toString() {
        lock.lock();
        try {
            return queue.toString();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            queue.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        lock.lock();
        try {
            return queue.remove(o);
        } finally {
            lock.unlock();
        }
    }

    private void checkInsert(int size) {
        Integer maxSize = this.maxCapacity.get();
        int currentSize = size();
        if (currentSize > maxSize - size) {
            throw new QueueOverflowException("Compute queue overflow when tried to insert " + size + " element(s) to queue. "
                    + "Current queue size is " + currentSize + ". "
                    + "Max queue size is " + maxSize + ".");
        }
    }
}
