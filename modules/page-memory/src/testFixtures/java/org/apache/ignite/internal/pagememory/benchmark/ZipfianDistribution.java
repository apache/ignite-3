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

import java.util.Arrays;
import java.util.Random;

/**
 * Zipfian distribution implementation for generating realistic hot/cold access patterns.
 *
 * <p>The Zipfian distribution follows a power law where the frequency of accessing
 * an item is inversely proportional to its rank. With high skew (e.g., 0.99),
 * this creates an 80/20 pattern where 20% of items receive 80% of accesses.
 *
 * <p>This implementation pre-computes cumulative probabilities for performance
 * and uses binary search to map uniform random values to Zipfian-distributed indices.
 *
 * <p>Thread-safe: Each instance should be used by a single thread. Use separate
 * instances (with different seeds) for multi-threaded scenarios.
 */
public class ZipfianDistribution {
    /** Number of items in the distribution. */
    private final int itemCount;

    /** Zipfian skew parameter (higher = more skewed, typical: 0.99). */
    private final double skew;

    /** Pre-computed cumulative probabilities for each item. */
    private final double[] cumulativeProbabilities;

    /** Random number generator (seedable for reproducibility). */
    private final Random random;

    /**
     * Creates a new Zipfian distribution.
     *
     * @param itemCount Number of items in the distribution (must be > 0).
     * @param skew Zipfian skew parameter (typical: 0.99 for 80/20 pattern).
     * @param seed Random seed for reproducibility.
     */
    public ZipfianDistribution(int itemCount, double skew, long seed) {
        if (itemCount <= 0) {
            throw new IllegalArgumentException("Item count must be positive: " + itemCount);
        }

        this.itemCount = itemCount;
        this.skew = skew;
        this.random = new Random(seed);

        // Pre-compute cumulative probabilities
        this.cumulativeProbabilities = new double[itemCount];
        double sum = 0.0;

        for (int i = 0; i < itemCount; i++) {
            // Probability for item i: 1 / (i+1)^skew
            sum += 1.0 / Math.pow(i + 1, skew);
            cumulativeProbabilities[i] = sum;
        }

        // Normalize to [0, 1]
        for (int i = 0; i < itemCount; i++) {
            cumulativeProbabilities[i] /= sum;
        }
    }

    /**
     * Returns the next item index according to the Zipfian distribution.
     *
     * @return Index in range [0, itemCount).
     */
    public int next() {
        // Generate uniform random value in [0, 1)
        double randomValue = random.nextDouble();

        // Binary search to find the item
        int index = Arrays.binarySearch(cumulativeProbabilities, randomValue);

        if (index < 0) {
            // binarySearch returns (-(insertion point) - 1) if not found
            index = -(index + 1);
        }

        // Ensure we don't exceed bounds due to floating point precision
        return Math.min(index, itemCount - 1);
    }

    /**
     * Returns the number of items in the distribution.
     *
     * @return Item count.
     */
    public int getItemCount() {
        return itemCount;
    }

    /**
     * Returns the skew parameter.
     *
     * @return Skew value.
     */
    public double getSkew() {
        return skew;
    }
}
