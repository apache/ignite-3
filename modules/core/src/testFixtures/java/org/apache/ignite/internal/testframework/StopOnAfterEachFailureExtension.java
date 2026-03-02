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

import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.TestOnly;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that stops all subsequent tests if an {@code @AfterEach} method fails.
 * This is particularly useful for integration tests where a failed cleanup (like cluster shutdown timeout)
 * can cause cascading failures in subsequent tests due to resource leaks (e.g., ports still in use).
 */
public class StopOnAfterEachFailureExtension implements
        AfterEachCallback,
        ExecutionCondition {

    private static final IgniteLogger LOG = Loggers.forClass(StopOnAfterEachFailureExtension.class);

    /** Global flag to track failures across ALL test classes. */
    private static volatile boolean globalAfterEachFailed = false;

    /** Global failure message with details. */
    private static volatile String globalFailureMessage = null;

    /**
     * Reset the global failure state. This is primarily for testing purposes.
     */
    @TestOnly
    static void resetGlobalState() {
        globalAfterEachFailed = false;
        globalFailureMessage = null;
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (context.getExecutionException().isPresent()) {
            Throwable exception = context.getExecutionException().get();

            String testName = context.getDisplayName();
            String testClassName = context.getTestClass().map(Class::getName).orElse("Unknown");
            String failureMsg = buildFailureMessage(testName, testClassName, exception);

            LOG.error("CRITICAL: @AfterEach failed in test '{}' ({}), aborting ALL remaining integration tests!",
                    testName, testClassName);
            LOG.error("Failure details: {}", failureMsg);
            LOG.error("This typically indicates resource leak (e.g., ports not released). "
                    + "All subsequent tests will be skipped to prevent cascade failures.");

            globalAfterEachFailed = true;
            globalFailureMessage = failureMsg;
        }
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (globalAfterEachFailed) {
            return ConditionEvaluationResult.disabled(
                "Test skipped because a previous test's @AfterEach cleanup failed in another test class. "
                        + "This prevents cascading failures across all integration tests. "
                        + "Original failure: " + globalFailureMessage
            );
        }

        return ConditionEvaluationResult.enabled("No previous @AfterEach failures detected");
    }

    /**
     * Builds a descriptive failure message.
     */
    private String buildFailureMessage(String testName, String testClassName, Throwable exception) {
        StringBuilder msg = new StringBuilder();
        msg.append("Test '").append(testName).append("' in class '").append(testClassName).append("' cleanup failed: ");
        msg.append(exception.getClass().getSimpleName()).append(": ");
        msg.append(exception.getMessage() != null ? exception.getMessage() : "(no message)");

        return msg.toString();
    }
}
