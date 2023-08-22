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

import static org.apache.ignite.internal.util.IgniteUtils.monotonicMs;
import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.lang.IgniteSystemProperties.getString;
import static org.mockito.Mockito.framework;

import java.lang.reflect.Method;
import java.nio.file.Path;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Ignite base test class.
 */
@ExtendWith(SystemPropertiesExtension.class)
public abstract class BaseIgniteAbstractTest {
    /** Logger. */
    protected static IgniteLogger log;

    /** Test start time in milliseconds. */
    private long testStartMs;

    static {
        /* Init test env. */
        S.setSensitiveDataLoggingPolicySupplier(() ->
                SensitiveDataLoggingPolicy.valueOf(getString(IGNITE_SENSITIVE_DATA_LOGGING, "hash").toUpperCase()));
    }

    /**
     * Should be invoked before a test will start.
     *
     * @param testInfo Test information object.
     * @param workDir Work directory.
     */
    protected void setupBase(TestInfo testInfo, @Nullable Path workDir) {
        log.info(">>> Starting test: {}#{}, displayName: {}, workDir: {}",
                testInfo.getTestClass().map(Class::getSimpleName).orElse("<null>"),
                testInfo.getTestMethod().map(Method::getName).orElse("<null>"),
                testInfo.getDisplayName(),
                workDir == null ? "<null>" : workDir.toAbsolutePath());

        this.testStartMs = monotonicMs();
    }

    /**
     * Should be invoked after the test has finished.
     *
     * @param testInfo Test information object.
     */
    protected void tearDownBase(TestInfo testInfo) {
        log.info(">>> Stopping test: {}#{}, displayName: {}, cost: {}ms.",
                testInfo.getTestClass().map(Class::getSimpleName).orElse("<null>"),
                testInfo.getTestMethod().map(Method::getName).orElse("<null>"),
                testInfo.getDisplayName(), monotonicMs() - testStartMs);
    }

    /**
     * Prevents accidental leaks from Mockito.
     */
    @AfterAll
    static void clearInlineMocks() {
        framework().clearInlineMocks();
    }

    /**
     * Constructor.
     */
    @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
    protected BaseIgniteAbstractTest() {
        log = Loggers.forClass(getClass());
    }

    /**
     * Returns logger.
     *
     * @return Logger.
     */
    protected static IgniteLogger logger() {
        return log;
    }
}
