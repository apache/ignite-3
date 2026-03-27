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
import static org.hamcrest.Matchers.nullValue;

import java.util.List;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;

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
     * Creates a matcher for public exceptions with stacktrace sent from the server.
     *
     * @param expectedClass expected exception type.
     * @param expectedCode expected code.
     * @param containMessage message that exception should contain.
     * @param causes Expected causes to be in the server sent stacktrace.
     * @return message that exception should contain.
     */
    public static TraceableExceptionMatcher publicException(
            Class<? extends TraceableException> expectedClass,
            int expectedCode,
            String containMessage,
            List<Cause> causes
    ) {
        var ret = traceableException(expectedClass)
                .withCode(is(expectedCode))
                .withMessage(containsString(containMessage))
                .withCause(nullValue(Throwable.class));

        for (var cause : causes) {
            if (cause.message() != null) {
                ret = ret.withMessage(containsString(String.format("Caused by: %s: %s", cause.className(), cause.message())));
            } else {
                ret = ret.withMessage(containsString(String.format("Caused by: %s", cause.className())));
            }
        }

        return ret;
    }

    /**
     * Creates an exception matcher with stacktrace not sent from the server.
     *
     * @param expectedClass expected exception type.
     * @param expectedCode expected code.
     * @param containMessage message that exception should contain.
     * @return message that exception should contain.
     */
    public static TraceableExceptionMatcher publicExceptionWithHint(
            Class<? extends TraceableException> expectedClass,
            int expectedCode,
            String containMessage
    ) {
        return traceableException(expectedClass)
                .withCode(is(expectedCode))
                .withMessage(containsString(containMessage))
                .withMessage(containsString("To see the full stack trace, "
                        + "set clientConnector.sendServerExceptionStackTraceToClient:true on the server"))
                .withCause(nullValue(Throwable.class));
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

    /** Cause. */
    public static class Cause {
        private final String className;

        // May be null, indicates no matcher will be used.
        @Nullable
        private final String message;

        public Cause(String className, @Nullable String message) {
            this.className = className;
            this.message = message;
        }

        public String className() {
            return className;
        }

        @Nullable
        public String message() {
            return message;
        }

        public static Cause of(Class<?> klass) {
            return new Cause(klass.getName(), null);
        }

        public static Cause of(Class<?> klass, String message) {
            return new Cause(klass.getName(), message);
        }
    }
}
