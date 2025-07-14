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

package org.apache.ignite.raft.jraft.util.concurrent;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** For {@link ConcurrentLinkedLifoByteBufferCollectorPool} testing. */
public class ConcurrentLinkedLifoByteBufferCollectorPoolTest {
    private static final int CAPACITY = 10;

    private final ConcurrentLinkedLifoByteBufferCollectorPool collectorPool = new ConcurrentLinkedLifoByteBufferCollectorPool(CAPACITY);

    @Test
    void borrowFromEmptyPool() {
        assertNull(collectorPool.borrow());
    }

    @Test
    void borrowSingleCollector() {
        ByteBufferCollector collector = allocateCollector();

        collectorPool.release(collector);

        assertSame(collector, collectorPool.borrow());
        assertNull(collectorPool.borrow());
    }

    @Test
    void borrowSameCollector() {
        ByteBufferCollector collector = allocateCollector();

        collectorPool.release(collector);
        collectorPool.release(collector);

        assertSame(collector, collectorPool.borrow());
        assertSame(collector, collectorPool.borrow());
        assertNull(collectorPool.borrow());
    }

    @Test
    void borrowSeveral() {
        List<ByteBufferCollector> collectors = allocateCollectors(5);

        collectors.forEach(collectorPool::release);

        assertEquals(reverse(collectors), borrowAll());
    }

    @RepeatedTest(10)
    void borrowConcurrent() {
        ByteBufferCollector collector0 = allocateCollector();
        ByteBufferCollector collector1 = allocateCollector();

        collectorPool.release(collector0);
        collectorPool.release(collector1);

        Set<ByteBufferCollector> borrows = ConcurrentHashMap.newKeySet();

        IgniteTestUtils.runRace(
                () -> borrows.add(collectorPool.borrow()),
                () -> borrows.add(collectorPool.borrow())
        );

        assertNull(collectorPool.borrow());

        assertThat(borrows, hasSize(2));
        assertThat(borrows, hasItems(collector0, collector1));
    }

    @Test
    void releaseMoreThanCapacity() {
        List<ByteBufferCollector> collectors = allocateCollectors(CAPACITY + 5);

        collectors.forEach(collectorPool::release);

        assertEquals(reverse(collectors.subList(0, CAPACITY)), borrowAll());
    }

    @RepeatedTest(10)
    void releaseConcurrent() {
        ByteBufferCollector collector0 = allocateCollector();
        ByteBufferCollector collector1 = allocateCollector();

        IgniteTestUtils.runRace(
                () -> collectorPool.release(collector0),
                () -> collectorPool.release(collector1)
        );

        List<ByteBufferCollector> borrowAll = borrowAll();
        assertThat(borrowAll, hasSize(2));
        assertThat(borrowAll, hasItems(collector0, collector1));
    }

    @RepeatedTest(10)
    void borrowReleaseConcurrent() {
        ByteBufferCollector collector0 = allocateCollector();
        ByteBufferCollector collector1 = allocateCollector();
        ByteBufferCollector collector2 = allocateCollector();
        ByteBufferCollector collector3 = allocateCollector();
        
        collectorPool.release(collector0);
        collectorPool.release(collector1);

        Set<ByteBufferCollector> borrows = ConcurrentHashMap.newKeySet();
        
        IgniteTestUtils.runRace(
                () -> collectorPool.release(collector2),
                () -> collectorPool.release(collector3),
                () -> borrows.add(collectorPool.borrow()),
                () -> borrows.add(collectorPool.borrow())
        );

        List<ByteBufferCollector> remainingInCollectorPool = borrowAll();
        
        assertThat(remainingInCollectorPool, hasSize(2));
        assertThat(borrows, hasSize(2));
        
        assertThat(remainingInCollectorPool, not(hasItems(borrows.toArray(ByteBufferCollector[]::new))));
    }

    private List<ByteBufferCollector> borrowAll() {
        var res = new ArrayList<ByteBufferCollector>();

        while (true) {
            ByteBufferCollector collector = collectorPool.borrow();

            if (collector == null) {
                break;
            } else {
                res.add(collector);
            }
        }

        return res;
    }

    private static ByteBufferCollector allocateCollector() {
        return ByteBufferCollector.allocate(32);
    }

    private static List<ByteBufferCollector> allocateCollectors(int size) {
        return IntStream.range(0, size)
                .mapToObj(i -> allocateCollector())
                .collect(toList());
    }

    private static List<ByteBufferCollector> reverse(List<ByteBufferCollector> l) {
        if (l.isEmpty()) {
            return l;
        }

        var res = new ArrayList<>(l);

        Collections.reverse(res);

        return res;
    }
}
