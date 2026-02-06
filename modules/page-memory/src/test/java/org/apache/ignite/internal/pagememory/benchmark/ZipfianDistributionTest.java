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

package org.apache.ignite.internal.pagememory.benchmark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZipfianDistribution}.
 */
class ZipfianDistributionTest {

    @Test
    void testZipfianProduces80_20Pattern() {
        // 10,000 items with high skew (0.99) should give ~80% accesses to top 20%
        int itemCount = 10_000;
        double skew = 0.99;
        ZipfianDistribution dist = new ZipfianDistribution(itemCount, skew, 42);

        // Sample 100k times
        int sampleSize = 100_000;
        int[] accessCounts = new int[itemCount];

        for (int i = 0; i < sampleSize; i++) {
            int index = dist.next();
            accessCounts[index]++;
        }

        // Sort indices by access count (descending)
        Integer[] sortedIndices = new Integer[itemCount];
        for (int i = 0; i < itemCount; i++) {
            sortedIndices[i] = i;
        }
        Arrays.sort(sortedIndices, (a, b) -> Integer.compare(accessCounts[b], accessCounts[a]));

        // Count accesses to top 20% of items
        int top20Count = itemCount / 5;  // 2000 items
        long top20Accesses = 0;
        for (int i = 0; i < top20Count; i++) {
            top20Accesses += accessCounts[sortedIndices[i]];
        }

        double top20Ratio = top20Accesses / (double) sampleSize;

        // Zipfian(0.99) should give ~80-85% to top 20%
        // We use 75%-90% to allow for statistical variance
        assertThat(
                String.format("Top 20%% of items should get 75%%-90%% of accesses, got %.1f%%",
                        top20Ratio * 100),
                top20Ratio,
                is(allOf(greaterThanOrEqualTo(0.75), lessThanOrEqualTo(0.90)))
        );
    }

    @Test
    void testProducesItemsWithinRange() {
        int itemCount = 100;
        ZipfianDistribution dist = new ZipfianDistribution(itemCount, 0.99, 42);

        for (int i = 0; i < 1_000; i++) {
            int index = dist.next();
            assertThat(
                    "Index should be within bounds [0," + itemCount + ")",
                    index,
                    is(allOf(greaterThanOrEqualTo(0), lessThan(itemCount)))
            );
        }
    }

    @Test
    void testSameSeedsProduceSameSequence() {
        ZipfianDistribution dist1 = new ZipfianDistribution(1000, 0.99, 42);
        ZipfianDistribution dist2 = new ZipfianDistribution(1000, 0.99, 42);

        for (int i = 0; i < 100; i++) {
            assertEquals(dist1.next(), dist2.next(), "Same seed should produce same sequence");
        }
    }

    @Test
    void testDifferentSeedsProduceDifferentSequences() {
        ZipfianDistribution dist1 = new ZipfianDistribution(1000, 0.99, 42);
        ZipfianDistribution dist2 = new ZipfianDistribution(1000, 0.99, 43);

        int differences = 0;
        for (int i = 0; i < 100; i++) {
            if (dist1.next() != dist2.next()) {
                differences++;
            }
        }

        // Different seeds should produce mostly different sequences
        assertThat("Different seeds should produce different sequences", differences, is(greaterThan(50)));
    }

    @Test
    void testLowerSkewIsLessConcentrated() {
        int itemCount = 10_000;
        int sampleSize = 100_000;

        // High skew (0.99) vs lower skew (0.5)
        ZipfianDistribution highSkew = new ZipfianDistribution(itemCount, 0.99, 42);
        ZipfianDistribution lowSkew = new ZipfianDistribution(itemCount, 0.5, 42);

        int[] highSkewCounts = new int[itemCount];
        int[] lowSkewCounts = new int[itemCount];

        for (int i = 0; i < sampleSize; i++) {
            highSkewCounts[highSkew.next()]++;
            lowSkewCounts[lowSkew.next()]++;
        }

        // Count how many items got at least 1 access
        int highSkewHitItems = 0;
        int lowSkewHitItems = 0;
        for (int i = 0; i < itemCount; i++) {
            if (highSkewCounts[i] > 0) {
                highSkewHitItems++;
            }
            if (lowSkewCounts[i] > 0) {
                lowSkewHitItems++;
            }
        }

        // Lower skew should hit more different items (less concentrated)
        assertThat(
                String.format("Lower skew should hit more items: low=%d, high=%d",
                        lowSkewHitItems, highSkewHitItems),
                lowSkewHitItems,
                is(greaterThan(highSkewHitItems))
        );
    }
}
