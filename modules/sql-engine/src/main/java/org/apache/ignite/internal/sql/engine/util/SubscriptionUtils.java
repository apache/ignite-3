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

package org.apache.ignite.internal.sql.engine.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.Flow.Publisher;

/**
 * Java Flow API utility methods.
 */
public class SubscriptionUtils {
    /**
     * Create thread-safe publisher wrapper of combine multiple publishers. Generally, start consuming a source once the previous source has
     * terminated, building a chain.
     *
     * @param sources Iterator which produces all publishers which should be combine.
     * @return Publisher which will be combine all of passed as parameter to single one.
     */
    public static <T> Publisher<T> concat(Iterator<Publisher<? extends T>> sources) {
        return new ConcatenatedPublisher<>(sources);
    }

    /**
     * Create thread-safe publisher wrapper of combine multiple publishers. Generally, start consuming a source once the previous source has
     * terminated, building a chain.
     *
     * @param sources Array of publishers which should be combine.
     * @return Publisher which will be combine all of passed as parameter to single one.
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
     * @param sources Array of upstream publishers. Each of passed publishers should have guarantee to be sorted with the same comparator.

     * @return Publisher which will be combine all of passed as parameter to single one with sorting guaranties.
     */
    @SafeVarargs
    public static <T> Publisher<T> orderedMerge(Comparator<T> comparator, int prefetch, Publisher<T>... sources) {
        return new OrderedMergePublisher<>(comparator, prefetch, sources);
    }
}
