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
 * Generates random numbers where some values come up much more often than others
 * (like real-world access patterns where a few pages are hot and most are cold).
 *
 * <p>Not thread-safe. Use separate instances per thread.
 */
public class ZipfianDistribution {
    private final int itemCount;
    private final double skew;
    private final double[] cumulativeProbabilities;
    private final Random random;

    /** Constructor. */
    public ZipfianDistribution(int itemCount, double skew, long seed) {
        if (itemCount <= 0) {
            throw new IllegalArgumentException("Item count must be positive: " + itemCount);
        }

        this.itemCount = itemCount;
        this.skew = skew;
        this.random = new Random(seed);

        this.cumulativeProbabilities = new double[itemCount];
        double sum = 0.0;

        for (int i = 0; i < itemCount; i++) {
            sum += 1.0 / Math.pow(i + 1, skew);
            cumulativeProbabilities[i] = sum;
        }

        for (int i = 0; i < itemCount; i++) {
            cumulativeProbabilities[i] /= sum;
        }
    }

    /** Returns next index. */
    public int next() {
        double randomValue = random.nextDouble();
        int index = Arrays.binarySearch(cumulativeProbabilities, randomValue);

        if (index < 0) {
            index = -(index + 1);
        }

        return Math.min(index, itemCount - 1);
    }

    public int getItemCount() {
        return itemCount;
    }

    public double getSkew() {
        return skew;
    }
}
