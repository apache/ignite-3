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

package org.apache.ignite.internal.testframework.asserts;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Assertions related to {@link CompletableFuture}.
 */
public class CompletableFutureAssert {
    /**
     * Asserts that the given future completes with an exception being an instance of the given class and returns
     * that exception for further examination.
     *
     * <p>Unlike {@link org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher#willThrowFast(Class)},
     * this method allows to examine the actual exception thrown further in the test.
     *
     * @param future Future to work on.
     * @param expectedExceptionClass Expected class of the exception.
     * @param <X> Exception type.
     * @return Matched exception.
     */
    public static <X extends Throwable> X assertWillThrowFast(
            CompletableFuture<?> future,
            Class<X> expectedExceptionClass
    ) {
        return assertWillThrow(future, expectedExceptionClass, 1, SECONDS);
    }

    /**
     * Asserts that the given future completes with an exception being an instance of the given class (in time) and returns
     * that exception for further examination.
     *
     * <p>Unlike
     * {@link org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher#willThrow(Class, int, TimeUnit)},
     * this method allows to examine the actual exception thrown further in the test.
     *
     * @param future Future to work on.
     * @param expectedExceptionClass Expected class of the exception.
     * @param timeout Duration to wait for future completion.
     * @param timeUnit Time unit of the duration.
     * @param <X> Exception type.
     * @return Matched exception.
     */
    public static <X extends Throwable> X assertWillThrow(
            CompletableFuture<?> future,
            Class<X> expectedExceptionClass,
            long timeout,
            TimeUnit timeUnit
    ) {
        Object normalResult;

        try {
            normalResult = future.get(timeout, timeUnit);
        } catch (TimeoutException e) {
            return fail("Expected the future to be completed with an exception of class in time, but it did not");
        } catch (Throwable e) {
            Throwable unwrapped = unwrapThrowable(e);

            if (expectedExceptionClass.isInstance(unwrapped)) {
                return (X) unwrapped;
            } else {
                return fail(
                        "Expected the future to be completed with an exception of class " + expectedExceptionClass.getName() + ", but got "
                                + e.getClass().getName(),
                        e
                );
            }
        }

        return fail("Expected the future to be completed with an exception of class " + expectedExceptionClass
                + ", but it completed normally with result " + normalResult);
    }

    private static Throwable unwrapThrowable(Throwable e) {
        while (e instanceof ExecutionException || e instanceof CompletionException) {
            e = e.getCause();
        }

        return e;
    }
}
