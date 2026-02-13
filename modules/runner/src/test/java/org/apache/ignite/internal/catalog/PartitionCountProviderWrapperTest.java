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

/** Unit tests for {@link PartitionCountProviderWrapper}. */
public class PartitionCountProviderWrapperTest extends BaseIgniteAbstractTest {
    /**
     * Tests that the default constructor uses the default partition count provider.
     */
    @Test
    void testDefaultConstructor() {
        PartitionCountProviderWrapper wrapper = new PartitionCountProviderWrapper();

        PartitionCountCalculationParameters params = PartitionCountCalculationParameters.builder().build();
        int result = wrapper.calculate(params);

        assertEquals(DEFAULT_PARTITION_COUNT, result);
    }

    /**
     * Tests that the constructor with custom provider correctly wraps it.
     */
    @Test
    void testConstructorWithCustomProvider() {
        int expectedPartitionCount = 42;

        AtomicInteger callCount = new AtomicInteger(0);

        PartitionCountProvider customProvider = params -> {
            callCount.incrementAndGet();

            return expectedPartitionCount;
        };

        PartitionCountProviderWrapper wrapper = new PartitionCountProviderWrapper(customProvider);

        PartitionCountCalculationParameters calculationParameters = PartitionCountCalculationParameters.builder().build();
        int result = wrapper.calculate(calculationParameters);

        assertEquals(expectedPartitionCount, result);
        assertNotEquals(expectedPartitionCount, DEFAULT_PARTITION_COUNT);
        assertEquals(1, callCount.get());

    }

    /**
     * Tests that calculate uses new provider after it has been changed several times.
     */
    @Test
    void testCalculateAfterProviderChange() {

        PartitionCountProviderWrapper wrapper = new PartitionCountProviderWrapper();
        PartitionCountCalculationParameters calculationParameters = PartitionCountCalculationParameters.builder().build();

        assertEquals(DEFAULT_PARTITION_COUNT, wrapper.calculate(calculationParameters));

        int expectedFirstlyChangedPartitionCount = 10;
        PartitionCountProvider firstChangeProvider = params -> expectedFirstlyChangedPartitionCount;
        wrapper.setPartitionCountProvider(firstChangeProvider);
        assertEquals(expectedFirstlyChangedPartitionCount, wrapper.calculate(calculationParameters));
        assertNotEquals(expectedFirstlyChangedPartitionCount, DEFAULT_PARTITION_COUNT);

        int expectedLastlyChangedPartitionCount = 20;
        PartitionCountProvider lastChangeProvider = params -> expectedLastlyChangedPartitionCount;
        wrapper.setPartitionCountProvider(lastChangeProvider);
        assertEquals(expectedLastlyChangedPartitionCount, wrapper.calculate(calculationParameters));
        assertNotEquals(expectedFirstlyChangedPartitionCount, DEFAULT_PARTITION_COUNT);
        assertNotEquals(expectedFirstlyChangedPartitionCount, expectedLastlyChangedPartitionCount);
    }

    /**
     * Tests that calculate works correctly with different parameter combinations.
     */
    @Test
    void testCalculateWithDifferentParameters() {
        PartitionCountProvider doubleProvider = params -> params.replicaFactor() * 2;

        PartitionCountProviderWrapper wrapper = new PartitionCountProviderWrapper(doubleProvider);

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
