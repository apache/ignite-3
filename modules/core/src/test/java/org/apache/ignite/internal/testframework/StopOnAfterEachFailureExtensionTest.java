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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.testkit.engine.EngineTestKit;

/**
 * Tests for {@link StopOnAfterEachFailureExtension}.
 */
class StopOnAfterEachFailureExtensionTest {

    @Test
    void testAllTestsRunWhenAfterEachSucceeds() {
        StopOnAfterEachFailureExtension.resetGlobalState();

        var results = EngineTestKit
                .engine("junit-jupiter")
                .selectors(DiscoverySelectors.selectClass(
                        TestClassWithSuccessfulAfterEach.class))
                .configurationParameter("junit.jupiter.conditions.deactivate", "org.junit.*DisabledCondition")
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
                .configurationParameter("junit.jupiter.conditions.deactivate", "org.junit.*DisabledCondition")
                .execute();

        results.testEvents()
                .assertStatistics(stats -> stats.started(1).skipped(1).failed(1));
    }

    /**
     * Test class where @AfterEach always succeeds.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-28031 Only for EngineTestKit execution")
    @ExtendWith(StopOnAfterEachFailureExtension.class)
    static class TestClassWithSuccessfulAfterEach {
        @AfterEach
        void cleanup() {
            // Always succeeds.
        }

        @Test
        void testA() {
        }

        @Test
        void testB() {
        }

        @Test
        void testC() {
        }
    }

    /**
     * Test class where @AfterEach times out.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-28031 Only for EngineTestKit execution")
    @ExtendWith(StopOnAfterEachFailureExtension.class)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    static class TestClassWithTimeoutInAfterEach {
        @AfterEach
        @Timeout(1)
        void cleanup() throws Exception {
            Thread.sleep(2000);
        }

        @Test
        @Order(1)
        void testOne() {
            // First test passes but cleanup times out.
        }

        @Test
        @Order(2)
        void testTwo() {
            // Should be skipped.
        }
    }
}
