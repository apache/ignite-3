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

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.RetriableTransactionException;
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
                .withCause(
                        // Checks if is either null or a RetriableException with no cause and same code and trace.
                        anyOf(
                                nullValue(Throwable.class),
                                allOf(
                                        instanceOf(RetriableTransactionException.class),
                                        traceableException(TraceableException.class)
                                                .withCode(is(expectedCode))
                                                .withCause(nullValue(Throwable.class))
                                )
                        )
                );

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

    /**
     * Creates a matcher that checks if a given exception complies with our public guidelines.
     *
     * @return Matcher.
     */
    public static Matcher<Exception> anyPublicException() {
        return allOf(
                anyOf(instanceOf(IgniteException.class), instanceOf(IgniteCheckedException.class)),
                traceableException(TraceableException.class)
                        .withCause(
                                // Checks if is either null or a RetriableException with no cause and same code and trace.
                                anyOf(
                                        nullValue(Throwable.class),
                                        allOf(
                                                instanceOf(RetriableTransactionException.class),
                                                traceableException(TraceableException.class)
                                                        .withCause(nullValue(Throwable.class))
                                        )
                                )
                        )
        );
    }

    /**
     * Wraps a KeyValueView with a proxy that checks that all exceptions thrown by it comply with the public guidelines.
     *
     * @param view View.
     * @param <K> KeyType.
     * @param <V> ValueType.
     * @return Proxy around the view.
     */
    public static <K, V> KeyValueView<K, V> withPublicExceptionAssertions(KeyValueView<K, V> view) {
        return (KeyValueView<K, V>) Proxy.newProxyInstance(
                KeyValueView.class.getClassLoader(),
                new Class<?>[]{KeyValueView.class},
                new PublicExceptionCheckInvocationHandler<>(view)
        );
    }

    /**
     * Wraps a IgniteSql with a proxy that checks that all exceptions thrown by it comply with the public guidelines.
     *
     * @param view IgniteSql instance..
     * @return Proxy around the IgniteSql.
     */
    public static IgniteSql withPublicExceptionAssertions(IgniteSql view) {
        return (IgniteSql) Proxy.newProxyInstance(
                IgniteSql.class.getClassLoader(),
                new Class<?>[]{IgniteSql.class},
                new PublicExceptionCheckInvocationHandler<>(view)
        );
    }

    private static class PublicExceptionCheckInvocationHandler<T> implements InvocationHandler {
        private final T target;

        PublicExceptionCheckInvocationHandler(T target) {
            this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            boolean isAsync = CompletableFuture.class.isAssignableFrom(method.getReturnType());

            if (isAsync) {
                try {
                    CompletableFuture<?> future = (CompletableFuture<?>) method.invoke(target, args);
                    return future.handle((r, e) -> {
                        if (e != null) {
                            Exception ex = (Exception) unwrapCause(e);
                            assertThat(ex, anyPublicException());
                            ExceptionUtils.sneakyThrow(e);
                        }

                        return r;
                    });
                } catch (InvocationTargetException e) {
                    return CompletableFuture.failedFuture(e);
                }
            } else {
                try {
                    return method.invoke(target, args);
                } catch (InvocationTargetException e) {
                    throw e;
                } catch (Exception e) {
                    assertThat(e, anyPublicException());
                    throw e;
                }
            }
        }
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
