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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class for queue's entries.
 * This class implement comparable mechanism for {@link BoundedPriorityBlockingQueue}
 * which used as queue in {@link java.util.concurrent.ThreadPoolExecutor}.
 * Each entry has unique seqNum which is used for comparing entries with identical priority.
 * It means that entries with same priority has FIFO resolving strategy in queue.
 *
 * @param <R> Compute job return type.
 */
public class QueueEntry<R> implements Runnable, Comparable<QueueEntry<R>> {
    private static final AtomicLong seq = new AtomicLong(Long.MIN_VALUE);

    private final CompletableFuture<R> future = new CompletableFuture<>();

    private final Callable<R> job;

    private final int priority;

    private final long seqNum;

    /**
     * Constructor.
     *
     * @param job Compute job callable.
     * @param priority Job priority.
     */
    public QueueEntry(Callable<R> job, int priority) {
        this.job = job;
        this.priority = priority;
        seqNum = seq.getAndIncrement();
    }

    @Override
    public void run() {
        try {
            future.complete(job.call());
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    /**
     * To {@link CompletableFuture} transformer.
     *
     * @return Completable future that will be finished when Compute job is finished.
     */
    CompletableFuture<R> toFuture() {
        return future;
    }

    @Override
    public int compareTo(QueueEntry o) {
        int compare = Integer.compare(o.priority, this.priority);
        if (compare == 0 && this != o) {
            return seqNum < o.seqNum ? -1 : 1;
        }
        return compare;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueueEntry<?> that = (QueueEntry<?>) o;

        return seqNum == that.seqNum;
    }

    @Override
    public int hashCode() {
        return (int) (seqNum ^ (seqNum >>> 32));
    }
}
