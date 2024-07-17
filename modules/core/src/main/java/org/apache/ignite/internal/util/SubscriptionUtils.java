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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.util.subscription.ConcatenatedPublisher;
import org.apache.ignite.internal.util.subscription.IterableToPublisherAdapter;
import org.apache.ignite.internal.util.subscription.OrderedMergePublisher;

/**
 * Java Flow API utility methods.
 */
public class SubscriptionUtils {
    /**
     * Creates a thread-safe publisher wrapper for a chain of multiple publishers. Generally, it consumes the next source after the previous
     * one completes, creating a chain.
     *
     * @param sources Iterator which produces all publishers which should be combined.
     * @return The publisher will combine all of the passed sources into a single one.
     */
    public static <T> Publisher<T> concat(Iterator<Publisher<? extends T>> sources) {
        return new ConcatenatedPublisher<>(sources);
    }

    /**
     * Creates a thread-safe publisher wrapper for a chain of multiple publishers. Generally, it consumes the next source after the previous
     * one completes, creating a chain.
     *
     * @param sources The array of publishers should be combined.
     * @return The publisher will combine all of the passed sources to single one.
     */
    @SafeVarargs
    public static <T> Publisher<T> concat(Publisher<? extends T>... sources) {
        return new ConcatenatedPublisher<>(Arrays.asList(sources).iterator());
    }

    /**
     * Sorting composite publisher. Merges multiple concurrent ordered data streams into one.
     *
     * @param comparator Rows comparator.
     * @param prefetch Prefetch size.
     * @param source Iterator of upstream publishers. Each of passed publishers should have guarantee to be sorted with the same
     *      comparator.
     * @return The publisher will combine all of the passed sources into a single one with sorting guaranties.
     */
    public static <T> Publisher<T> orderedMerge(Comparator<T> comparator, int prefetch, Iterator<Publisher<? extends T>> source) {
        List<Publisher<? extends T>> pubList = new ArrayList<>();
        source.forEachRemaining(pubList::add);

        return orderedMerge(comparator, prefetch, pubList.<Publisher<T>>toArray(Publisher[]::new));
    }

    /**
     * Sorting composite publisher. Merges multiple concurrent ordered data streams into one.
     *
     * @param comparator Rows comparator.
     * @param prefetch Prefetch size.
     * @param sources Array of upstream publishers. Each of passed publishers should have guarantee to be sorted with the same comparator.
     * @return The publisher will combine all of the passed sources into a single one with sorting guaranties.
     */
    @SafeVarargs
    public static <T> Publisher<T> orderedMerge(Comparator<T> comparator, int prefetch, Publisher<? extends T>... sources) {
        return new OrderedMergePublisher<>(comparator, prefetch, sources);
    }

    /**
     * Creates a publisher from the given iterable.
     *
     * <p>A new iterator will be issued for every new subscription.
     *
     * <p>This particular adapter will drain iterator on the same thread that requested the
     * entries, so it's better to avoid using long or blocking operations inside provided iterable.
     *
     * @param iterable An iterable to create adapter for.
     * @param <T> Type of the entries this publisher will emit.
     * @return Publisher created from the given iterable.
     */
    public static <T> Publisher<T> fromIterable(Iterable<T> iterable) {
        return new IterableToPublisherAdapter<>(iterable, Runnable::run, Integer.MAX_VALUE);
    }
}
