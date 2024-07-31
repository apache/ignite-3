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

import static org.apache.ignite.internal.lang.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getString;
import static org.apache.ignite.internal.util.IgniteUtils.monotonicMs;

import java.lang.reflect.Method;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * Ignite base test class.
 */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = IgniteSystemProperties.THREAD_ASSERTIONS_ENABLED, value = "true")
public abstract class BaseIgniteAbstractTest {
    /** Logger. */
    protected final IgniteLogger log = Loggers.forClass(getClass());

    /** Test start time in milliseconds. */
    private long testStartMs;

    @BeforeAll
    static void setLoggingPolicy() {
        S.setSensitiveDataLoggingPolicySupplier(() -> {
            String loggingPolicy = getString(IGNITE_SENSITIVE_DATA_LOGGING, "hash");

            return SensitiveDataLoggingPolicy.valueOf(loggingPolicy.toUpperCase());
        });
    }

    /**
     * Prevents accidental leaks from Mockito.
     */
    @AfterAll
    static void clearInlineMocks() {
        Mockito.framework().clearInlineMocks();
    }

    /** For tests with paranoid netty leak detection forces GC to catch leaks. */
    @AfterAll
    static void forceGcForNettyBufferLeaksDetection() {
        if ("paranoid".equals(System.getProperty("io.netty.leakDetectionLevel"))) {
            //noinspection CallToSystemGC
            System.gc();
        }
    }

    @BeforeEach
    void printStartMessage(TestInfo testInfo) {
        log.info(">>> Starting test: {}#{}, displayName: {}",
                testInfo.getTestClass().map(Class::getSimpleName).orElse("<null>"),
                testInfo.getTestMethod().map(Method::getName).orElse("<null>"),
                testInfo.getDisplayName()
        );

        this.testStartMs = monotonicMs();
    }

    @AfterEach
    void printStopMessage(TestInfo testInfo) {
        log.info(">>> Stopping test: {}#{}, displayName: {}, cost: {}ms.",
                testInfo.getTestClass().map(Class::getSimpleName).orElse("<null>"),
                testInfo.getTestMethod().map(Method::getName).orElse("<null>"),
                testInfo.getDisplayName(),
                monotonicMs() - testStartMs
        );
    }

    /**
     * Returns logger.
     *
     * @return Logger.
     */
    protected IgniteLogger logger() {
        return log;
    }
}
