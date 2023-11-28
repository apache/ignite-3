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

package org.apache.ignite.internal.table.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Base tests for {@link PartitionSet}. */
abstract class AbstractPartitionSetTest {
    private PartitionSet partitionSet;

    @BeforeEach
    void setUp() {
        partitionSet = createSet();
    }

    @Test
    public void testSetPartition() {
        int partCount = 65_000;

        // Set every even partition.
        for (int i = 0; i < partCount; i++) {
            if ((i % 2) == 0) {
                partitionSet.set(i);
            }
        }

        // Check that only partitions that were set are actually set.
        for (int i = 0; i < partCount; i++) {
            assertEquals((i % 2) == 0, partitionSet.get(i));
        }
    }

    @Test
    public void testClearPartition() {
        int partCount = 65_000;

        // Set every partition.
        for (int i = 0; i < partCount; i++) {
            partitionSet.set(i);
        }

        // Clear every even partition.
        for (int i = 0; i < partCount; i++) {
            if ((i % 2) == 0) {
                partitionSet.clear(i);
            }
        }

        // Check that only odd partitions are set.
        for (int i = 0; i < partCount; i++) {
            assertEquals((i % 2) != 0, partitionSet.get(i));
        }
    }

    @Test
    public void testStream() {
        int partCount = 65_000;

        int setPartitionsCount = 0;

        // Set every even partition.
        for (int i = 0; i < partCount; i++) {
            if ((i % 2) == 0) {
                partitionSet.set(i);
                setPartitionsCount++;
            }
        }

        var realCount = new AtomicInteger();

        // Check that only partitions that were set are actually set.
        partitionSet.stream().forEach(value -> {
            assertEquals(0, value % 2);
            realCount.incrementAndGet();
        });

        assertEquals(setPartitionsCount, realCount.get());
    }

    /**
     * Tests that copy is equal, but is not the same, as the copied set.
     */
    @Test
    public void testCopy() {
        PartitionSet copy = partitionSet.copy();

        assertEquals(partitionSet, copy);
        assertEquals(partitionSet.hashCode(), copy.hashCode());
        assertNotSame(partitionSet, copy);
    }

    /**
     * Tests that empty sets of different classes are equal.
     */
    @SuppressWarnings("MisorderedAssertEqualsArguments")
    @Test
    public void testEmptyEqual() {
        assertEquals(partitionSet, PartitionSet.EMPTY_SET);
        assertEquals(PartitionSet.EMPTY_SET, partitionSet);

        assertEquals(partitionSet.hashCode(), PartitionSet.EMPTY_SET.hashCode());
        assertEquals(PartitionSet.EMPTY_SET.hashCode(), partitionSet.hashCode());
    }

    @Test
    public void testDifferentNotEqual() {
        partitionSet.set(0);

        PartitionSet anotherSet = createSet();
        anotherSet.set(1);

        assertNotEquals(partitionSet, anotherSet);
    }

    abstract PartitionSet createSet();
}
