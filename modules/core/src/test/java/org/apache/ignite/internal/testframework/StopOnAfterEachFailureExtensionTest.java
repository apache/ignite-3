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

package org.apache.ignite.internal.testframework;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.testkit.engine.EngineTestKit;

/**
 * Tests for {@link StopOnAfterEachFailureExtension}.
 */
class StopOnAfterEachFailureExtensionTest {

    @Test
    void testAfterEachFailureDetected() {
        StopOnAfterEachFailureExtension.resetGlobalState();

        var results = EngineTestKit
                .engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(
                        TestClassWithFailingAfterEach.class))
                .execute();

        results.testEvents()
                .assertStatistics(stats -> stats.started(1).skipped(2).failed(1));
    }

    @Test
    void testAllTestsRunWhenAfterEachSucceeds() {
        StopOnAfterEachFailureExtension.resetGlobalState();

        var results = EngineTestKit
                .engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(
                        TestClassWithSuccessfulAfterEach.class))
                .execute();

        results.testEvents()
                .assertStatistics(stats -> stats.started(3).succeeded(3).skipped(0));
    }

    @Test
    void testAfterEachTimeoutDetected() {
        StopOnAfterEachFailureExtension.resetGlobalState();

        var results = EngineTestKit
                .engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(
                        TestClassWithTimeoutInAfterEach.class))
                .execute();

        results.testEvents()
                .assertStatistics(stats -> stats.started(1).skipped(1).failed(1));
    }

    @Test
    void testExtensionAffectsAllTestClassesGlobally() {
        StopOnAfterEachFailureExtension.resetGlobalState();

        var results = EngineTestKit
                .engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(
                        TestClassWithSuccessfulAfterEach.class))
                .execute();

        results.testEvents()
                .assertStatistics(stats -> stats.started(3).succeeded(3).skipped(0));
    }

    // ==================== Test Classes ====================

    /**
     * Test class where @AfterEach fails after first test.
     */
    @ExtendWith(StopOnAfterEachFailureExtension.class)
    static class TestClassWithFailingAfterEach {
        private static int testCount = 0;
        private static final List<String> executedTests = new ArrayList<>();

        @BeforeEach
        void setup() {
            testCount++;
        }

        @AfterEach
        void cleanup() {
            // Fail after first test.
            if (testCount == 1) {
                throw new RuntimeException("Simulated cleanup failure");
            }
        }

        @Test
        void firstTest() {
            executedTests.add("firstTest");
        }

        @Test
        void secondTest() {
            executedTests.add("secondTest");
        }

        @Test
        void thirdTest() {
            executedTests.add("thirdTest");
        }
    }

    /**
     * Test class where @AfterEach always succeeds.
     */
    @ExtendWith(StopOnAfterEachFailureExtension.class)
    static class TestClassWithSuccessfulAfterEach {
        private static final List<String> executedTests = new ArrayList<>();

        @AfterEach
        void cleanup() {
            // Always succeeds.
        }

        @Test
        void testA() {
            executedTests.add("testA");
        }

        @Test
        void testB() {
            executedTests.add("testB");
        }

        @Test
        void testC() {
            executedTests.add("testC");
        }
    }

    /**
     * Test class where @AfterEach times out.
     */
    @ExtendWith(StopOnAfterEachFailureExtension.class)
    static class TestClassWithTimeoutInAfterEach {
        private static int testCount = 0;

        @BeforeEach
        void setup() {
            testCount++;
        }

        @AfterEach
        void cleanup() throws Exception {
            // Simulate timeout after first test.
            if (testCount == 1) {
                throw new TimeoutException("Cleanup timed out after 60 seconds");
            }
        }

        @Test
        void testOne() {
            // First test passes but cleanup times out.
        }

        @Test
        void testTwo() {
            // Should be skipped.
        }
    }
}
