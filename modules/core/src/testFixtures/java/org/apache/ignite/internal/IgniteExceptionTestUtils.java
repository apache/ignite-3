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

package org.apache.ignite.internal;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * Test utils for checking public exceptions.
 */
public class IgniteExceptionTestUtils {

    /**
     * Creates a matcher that matches a traceable exception with expected type.
     *
     * @param expectedType expected exception type.
     */
    public static TraceableExceptionMatcher traceableException(Class<? extends TraceableException> expectedType) {
        return new TraceableExceptionMatcher(expectedType);
    }

    /**
     * Creates a matcher that matches a traceable exception with expected type, code and message.
     *
     * @param expectedType expected exception type.
     * @param expectedCode expected code.
     * @param containMessage message that exception should contain.
     */
    public static TraceableExceptionMatcher traceableException(
            Class<? extends TraceableException> expectedType,
            int expectedCode,
            String containMessage
    ) {
        return traceableException(expectedType)
                .withCode(is(expectedCode))
                .withMessage(containsString(containMessage));
    }

    /**
     * Creates a matcher that matches a public exception with expected code and message.
     *
     * @param expectedCode expected code.
     * @param containMessage message that exception should contain.
     */
    public static TraceableExceptionMatcher publicException(int expectedCode, String containMessage) {
        return traceableException(IgniteException.class, expectedCode, containMessage);
    }

    /**
     * Creates a matcher that matches a public checked exception with expected code and message.
     *
     * @param expectedCode expected code.
     * @param containMessage message that exception should contain.
     */
    public static TraceableExceptionMatcher publicCheckedException(int expectedCode, String containMessage) {
        return traceableException(IgniteCheckedException.class, expectedCode, containMessage);
    }

    /**
     * Creates a matcher that matches an exception with the message matching specified matcher.
     *
     * @param messageMatcher Matcher to match message with.
     */
    public static Matcher<Throwable> hasMessage(Matcher<String> messageMatcher) {
        return new FeatureMatcher<>(messageMatcher, "a throwable with message", "message") {
            @Override
            protected String featureValueOf(Throwable actual) {
                return actual.getMessage();
            }
        };
    }
}
