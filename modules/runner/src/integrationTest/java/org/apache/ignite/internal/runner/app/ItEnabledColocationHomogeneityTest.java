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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.cluster.management.InvalidNodeConfigurationException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests to check enabled colocation homogeneity within node join validation.
 */
@SuppressWarnings("ThrowableNotThrown")
@Disabled("https://issues.apache.org/jira/browse/IGNITE-27071")
public class ItEnabledColocationHomogeneityTest extends BaseIgniteRestartTest {
    private String commonColocationFeatureFlag;

    @BeforeEach
    public void setUp() {
        commonColocationFeatureFlag = System.getProperty(COLOCATION_FEATURE_FLAG);
    }

    @AfterEach
    public void tearDown() {
        if (commonColocationFeatureFlag == null) {
            System.clearProperty(COLOCATION_FEATURE_FLAG);
        } else {
            System.setProperty(COLOCATION_FEATURE_FLAG, commonColocationFeatureFlag);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testJoinDeniedIsThrownInCaseOfIncompatibleColocationModes(boolean colocationEnabled) {
        System.setProperty(COLOCATION_FEATURE_FLAG, Boolean.toString(colocationEnabled));
        startNode(0);

        System.setProperty(COLOCATION_FEATURE_FLAG, Boolean.toString(!colocationEnabled));
        assertThrowsWithCause(
                () -> startNode(1),
                InvalidNodeConfigurationException.class,
                IgniteStringFormatter.format("Colocation enabled mode does not match. Joining node colocation mode is: {},"
                        + " cluster colocation mode is: {}", !colocationEnabled, colocationEnabled)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testJoinDeniedIsNotThrownInCaseOfCompatibleColocationModes(boolean colocationEnabled) {
        System.setProperty(COLOCATION_FEATURE_FLAG, Boolean.toString(colocationEnabled));
        startNode(0);
        startNode(1);
    }
}
