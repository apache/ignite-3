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

package org.apache.ignite.internal.hlc;

import static org.apache.ignite.internal.hlc.HybridTimestamp.PHYSICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.max;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests of a hybrid timestamp implementation.
 * {@link HybridTimestamp}
 */
class HybridTimestampTest {
    @Test
    void testComparison() {
        assertEquals(new HybridTimestamp(10, 5),
                max(new HybridTimestamp(10, 5), new HybridTimestamp(5, 7))
        );

        assertEquals(new HybridTimestamp(20, 10),
                max(new HybridTimestamp(10, 100), new HybridTimestamp(20, 10))
        );

        assertEquals(new HybridTimestamp(20, 10),
                max(new HybridTimestamp(20, 10))
        );
    }

    @Test
    void equalWhenComponentsAreSame() {
        assertThat(new HybridTimestamp(1, 2), equalTo(new HybridTimestamp(1, 2)));
    }

    @Test
    void notEqualWhenPhysicalIsDifferent() {
        assertThat(new HybridTimestamp(1, 2), not(equalTo(new HybridTimestamp(2, 2))));
    }

    @Test
    void notEqualWhenLogicalIsDifferent() {
        assertThat(new HybridTimestamp(1, 2), not(equalTo(new HybridTimestamp(1, 3))));
    }

    @Test
    void hashCodeSameWhenComponentsAreSame() {
        assertEquals(new HybridTimestamp(1, 2).hashCode(), new HybridTimestamp(1, 2).hashCode());
    }

    @Test
    void addPhysicalTimeIncrementsPhysicalComponent() {
        HybridTimestamp before = new HybridTimestamp(1, 2);

        HybridTimestamp after = before.addPhysicalTime(1000);

        assertThat(after.getPhysical(), is(1001L));
    }

    @Test
    void addPhysicalTimeLeavesLogicalComponentIntact() {
        HybridTimestamp before = new HybridTimestamp(1, 2);

        HybridTimestamp after = before.addPhysicalTime(1000);

        assertThat(after.getLogical(), is(2));
    }

    @Test
    void addPhysicalTimeThrowsIfIncrementIsTooBig() {
        HybridTimestamp before = new HybridTimestamp(1, 2);

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> before.addPhysicalTime(1L << PHYSICAL_TIME_BITS_SIZE)
        );
        assertThat(ex.getMessage(), is("Physical time is out of bounds: 281474976710656"));
    }

    @Test
    void addPhysicalTimeThrowsIfOverflowHappens() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> HybridTimestamp.MAX_VALUE.addPhysicalTime(1)
        );
        assertThat(ex.getMessage(), startsWith("Time is out of bounds: "));
    }

    @Test
    void subtractPhysicalTimeDecrementsPhysicalComponent() {
        HybridTimestamp before = new HybridTimestamp(2001, 2);

        HybridTimestamp after = before.subtractPhysicalTime(1000);

        assertThat(after.getPhysical(), is(1001L));
    }

    @Test
    void subtractPhysicalTimeLeavesLogicalComponentIntact() {
        HybridTimestamp before = new HybridTimestamp(2001, 2);

        HybridTimestamp after = before.subtractPhysicalTime(1000);

        assertThat(after.getLogical(), is(2));
    }

    @Test
    void subtractPhysicalTimeThrowsIfDecrementIsTooBig() {
        HybridTimestamp before = new HybridTimestamp(2001, 2);

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> before.addPhysicalTime(1L << PHYSICAL_TIME_BITS_SIZE)
        );
        assertThat(ex.getMessage(), is("Physical time is out of bounds: 281474976710656"));
    }

    @Test
    void subtractPhysicalTimeThrowsIfUnderflowHappens() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> HybridTimestamp.MIN_VALUE.subtractPhysicalTime(1_000_000)
        );
        assertThat(ex.getMessage(), startsWith("Time is out of bounds: "));
    }

    @Test
    void roundUpLeavesTsUnchangedWhenLogicalPartIsZero() {
        var ts = new HybridTimestamp(1, 0);

        assertThat(ts.roundUpToPhysicalTick(), is(ts));
    }

    @Test
    void roundUpIncrementsPhysicalPartWhenLogicalPartIsNonZero() {
        var ts = new HybridTimestamp(1, 1);

        assertThat(ts.roundUpToPhysicalTick(), is(new HybridTimestamp(2, 0)));
    }
}
