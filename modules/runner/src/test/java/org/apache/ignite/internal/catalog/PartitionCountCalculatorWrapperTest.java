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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link PartitionCountCalculatorWrapper}. */
public class PartitionCountCalculatorWrapperTest extends BaseIgniteAbstractTest {
    /**
     * Tests that the default constructor uses the default partition count calculator.
     */
    @Test
    void testDefaultConstructor() {
        PartitionCountCalculatorWrapper wrapper = new PartitionCountCalculatorWrapper();

        PartitionCountCalculationParameters params = PartitionCountCalculationParameters.builder().build();
        int result = wrapper.calculate(params);

        assertEquals(DEFAULT_PARTITION_COUNT, result);
    }

    /**
     * Tests that the constructor with custom partition calculator correctly wraps it.
     */
    @Test
    void testConstructorWithCustomPartitionCalculator() {
        int expectedPartitionCount = 42;

        AtomicInteger callCount = new AtomicInteger(0);

        PartitionCountCalculator customCalculator = params -> {
            callCount.incrementAndGet();

            return expectedPartitionCount;
        };

        PartitionCountCalculatorWrapper wrapper = new PartitionCountCalculatorWrapper(customCalculator);

        PartitionCountCalculationParameters calculationParameters = PartitionCountCalculationParameters.builder().build();
        int result = wrapper.calculate(calculationParameters);

        assertEquals(expectedPartitionCount, result);
        assertNotEquals(expectedPartitionCount, DEFAULT_PARTITION_COUNT);
        assertEquals(1, callCount.get());

    }

    /**
     * Tests that calculate uses new partition calculator after it has been changed several times.
     */
    @Test
    void testCalculateAfterPartitionCalculatorChange() {

        PartitionCountCalculatorWrapper wrapper = new PartitionCountCalculatorWrapper();
        PartitionCountCalculationParameters calculationParameters = PartitionCountCalculationParameters.builder().build();

        assertEquals(DEFAULT_PARTITION_COUNT, wrapper.calculate(calculationParameters));

        int expectedFirstlyChangedPartitionCount = 10;
        PartitionCountCalculator firstChangeCalculator = params -> expectedFirstlyChangedPartitionCount;
        wrapper.setPartitionCountCalculator(firstChangeCalculator);
        assertEquals(expectedFirstlyChangedPartitionCount, wrapper.calculate(calculationParameters));
        assertNotEquals(expectedFirstlyChangedPartitionCount, DEFAULT_PARTITION_COUNT);

        int expectedLastlyChangedPartitionCount = 20;
        PartitionCountCalculator lastChangeCalculator = params -> expectedLastlyChangedPartitionCount;
        wrapper.setPartitionCountCalculator(lastChangeCalculator);
        assertEquals(expectedLastlyChangedPartitionCount, wrapper.calculate(calculationParameters));
        assertNotEquals(expectedFirstlyChangedPartitionCount, DEFAULT_PARTITION_COUNT);
        assertNotEquals(expectedFirstlyChangedPartitionCount, expectedLastlyChangedPartitionCount);
    }

    /**
     * Tests that calculate function works correctly with different parameter combinations.
     */
    @Test
    void testCalculateWithDifferentParameters() {
        PartitionCountCalculator doubleCalculator = params -> params.replicaFactor() * 2;

        PartitionCountCalculatorWrapper wrapper = new PartitionCountCalculatorWrapper(doubleCalculator);

        PartitionCountCalculationParameters params1 = PartitionCountCalculationParameters.builder()
                .replicaFactor(2)
                .build();
        assertEquals(4, wrapper.calculate(params1));

        PartitionCountCalculationParameters params2 = PartitionCountCalculationParameters.builder()
                .replicaFactor(5)
                .build();
        assertEquals(10, wrapper.calculate(params2));

        PartitionCountCalculationParameters params3 = PartitionCountCalculationParameters.builder()
                .replicaFactor(10)
                .build();
        assertEquals(20, wrapper.calculate(params3));
    }
}
