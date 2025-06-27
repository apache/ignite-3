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

package org.apache.ignite.internal.lang;

import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCauseOrSuppressed;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests exception util methods.
 */
public class ExceptionUtilsTest {
    @ParameterizedTest
    @MethodSource("provideIgniteExceptions")
    public <T extends IgniteException> void testPublicException(T origin) {
        var completionErr = new CompletionException(origin);

        Throwable tcopy = ExceptionUtils.copyExceptionWithCause(completionErr);

        assertThat(tcopy, isA(origin.getClass()));

        T copy = (T) tcopy;

        assertThat(copy.code(), is(origin.code()));
        assertThat(copy.traceId(), is(origin.traceId()));
        assertThat(copy.getMessage(), is(origin.getMessage()));
    }

    @ParameterizedTest
    @MethodSource("provideIgniteCheckedExceptions")
    public <T extends IgniteCheckedException> void testPublicCheckedException(T origin) {
        var completionErr = new CompletionException(origin);

        Throwable tcopy = ExceptionUtils.copyExceptionWithCause(completionErr);

        assertThat(tcopy, isA(origin.getClass()));

        T copy = (T) tcopy;

        assertThat(copy.code(), is(origin.code()));
        assertThat(copy.traceId(), is(origin.traceId()));
        assertThat(copy.getMessage(), is(origin.getMessage()));
    }

    private static Stream<IgniteException> provideIgniteExceptions() {
        return Stream.of(
                new TestException(),
                new TestExceptionWithCode(NODE_STOPPING_ERR),
                new TestExceptionWithMessage("test message"),
                new TestExceptionWithCause(new IllegalArgumentException("test message")),
                new TestExceptionWithCodeAndMessage(NODE_STOPPING_ERR, "test message"),
                new TestExceptionWithCodeAndCause(NODE_STOPPING_ERR, new TestException()),
                new TestExceptionWithCodeMessageAndCause(NODE_STOPPING_ERR, "test message", new IllegalArgumentException())
        );
    }

    private static Stream<IgniteCheckedException> provideIgniteCheckedExceptions() {
        return Stream.of(
                new TestCheckedException(),
                new TestCheckedExceptionWithCode(NODE_STOPPING_ERR),
                new TestCheckedExceptionWithMessage("test message"),
                new TestCheckedExceptionWithCause(new IllegalArgumentException("test message")),
                new TestCheckedExceptionWithCodeAndMessage(NODE_STOPPING_ERR, "test message"),
                new TestCheckedExceptionWithCodeAndCause(NODE_STOPPING_ERR, new TestException()),
                new TestCheckedExceptionWithCodeMessageAndCause(NODE_STOPPING_ERR, "test message", new IllegalArgumentException())
        );
    }

    @Test
    void withCauseDoesNotApplyDefaultCodeWhenCodeIsThere() {
        TraceableException translated = ExceptionUtils.withCause(
                TestUncheckedExceptionWithTraceCodeAndCause::new,
                Transactions.TX_COMMIT_ERR,
                new IgniteException(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR)
        );

        assertThat(translated.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));
    }

    @Test
    void withCauseDoesNotApplyDefaultCodeWhenCodeIsInExceptionWrappedInExecutionException() {
        TraceableException translated = ExceptionUtils.withCause(
                TestUncheckedExceptionWithTraceCodeAndCause::new,
                Transactions.TX_COMMIT_ERR,
                new ExecutionException(new IgniteException(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR))
        );

        assertThat(translated.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));
    }

    @Test
    void withCauseDoesNotApplyDefaultCodeWhenCodeIsInExceptionWrappedInCompletionException() {
        TraceableException translated = ExceptionUtils.withCause(
                TestUncheckedExceptionWithTraceCodeAndCause::new,
                Transactions.TX_COMMIT_ERR,
                new CompletionException(new IgniteException(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR))
        );

        assertThat(translated.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));
    }

    @Test
    void withCauseAppliesDefaultCodeWhenHandlingNonIgniteException() {
        TraceableException translated = ExceptionUtils.withCause(
                TestUncheckedExceptionWithTraceCodeAndCause::new,
                Transactions.TX_COMMIT_ERR,
                new RuntimeException()
        );

        assertThat(translated.code(), is(Transactions.TX_COMMIT_ERR));
    }

    @Test
    void hasCauseOrSuppressedWithoutMessage() {
        var ex0 = new Exception();
        var ex1 = new RuntimeException(ex0);

        assertFalse(hasCauseOrSuppressed(ex0, AssertionError.class));
        assertFalse(hasCauseOrSuppressed(ex1, AssertionError.class));

        assertTrue(hasCauseOrSuppressed(ex0, Exception.class));
        assertFalse(hasCauseOrSuppressed(ex0, RuntimeException.class));

        assertTrue(hasCauseOrSuppressed(ex1, Exception.class));
        assertTrue(hasCauseOrSuppressed(ex1, RuntimeException.class));
    }

    @Test
    void hasCauseOrSuppressedWithMessage() {
        var ex0 = new Exception("ex0");
        var ex1 = new RuntimeException("ex1", ex0);

        assertFalse(hasCauseOrSuppressed(ex0, AssertionError.class));
        assertFalse(hasCauseOrSuppressed(ex1, AssertionError.class));

        assertFalse(hasCauseOrSuppressed(ex0, "foo", Exception.class));
        assertFalse(hasCauseOrSuppressed(ex1, "bar", RuntimeException.class));

        assertTrue(hasCauseOrSuppressed(ex0, "e", Exception.class));
        assertTrue(hasCauseOrSuppressed(ex0, "ex", Exception.class));
        assertTrue(hasCauseOrSuppressed(ex0, "ex0", Exception.class));

        assertTrue(hasCauseOrSuppressed(ex1, "ex", Exception.class));
        assertTrue(hasCauseOrSuppressed(ex1, "e", RuntimeException.class));
        assertTrue(hasCauseOrSuppressed(ex1, "ex", RuntimeException.class));
        assertTrue(hasCauseOrSuppressed(ex1, "ex1", RuntimeException.class));
    }

    @Test
    void hasCauseOrSuppressedWithCyclicDependency() {
        var ex0 = new Exception();
        var ex1 = new RuntimeException(ex0);

        ex0.initCause(ex1);

        assertFalse(hasCauseOrSuppressed(ex0, AssertionError.class));
        assertFalse(hasCauseOrSuppressed(ex1, AssertionError.class));

        assertTrue(hasCauseOrSuppressed(ex0, Exception.class));
        assertTrue(hasCauseOrSuppressed(ex0, RuntimeException.class));

        assertTrue(hasCauseOrSuppressed(ex1, Exception.class));
        assertTrue(hasCauseOrSuppressed(ex1, RuntimeException.class));
    }

    @Test
    void hasCauseOrSuppressedWithoutMessageInSuppressed() {
        var ex0 = new Exception();
        var ex1 = new RuntimeException();

        ex1.addSuppressed(ex0);

        assertFalse(hasCauseOrSuppressed(ex0, AssertionError.class));
        assertFalse(hasCauseOrSuppressed(ex1, AssertionError.class));

        assertTrue(hasCauseOrSuppressed(ex0, Exception.class));
        assertFalse(hasCauseOrSuppressed(ex0, RuntimeException.class));

        assertTrue(hasCauseOrSuppressed(ex1, Exception.class));
        assertTrue(hasCauseOrSuppressed(ex1, RuntimeException.class));
    }

    @Test
    void hasCauseOrSuppressedWithMessageInSuppressed() {
        var ex0 = new Exception("ex0");
        var ex1 = new RuntimeException("ex1", ex0);

        ex1.addSuppressed(ex0);

        assertFalse(hasCauseOrSuppressed(ex0, AssertionError.class));
        assertFalse(hasCauseOrSuppressed(ex1, AssertionError.class));

        assertFalse(hasCauseOrSuppressed(ex0, "foo", Exception.class));
        assertFalse(hasCauseOrSuppressed(ex1, "bar", RuntimeException.class));

        assertTrue(hasCauseOrSuppressed(ex0, "e", Exception.class));
        assertTrue(hasCauseOrSuppressed(ex0, "ex", Exception.class));
        assertTrue(hasCauseOrSuppressed(ex0, "ex0", Exception.class));

        assertTrue(hasCauseOrSuppressed(ex1, "ex", Exception.class));
        assertTrue(hasCauseOrSuppressed(ex1, "e", RuntimeException.class));
        assertTrue(hasCauseOrSuppressed(ex1, "ex", RuntimeException.class));
        assertTrue(hasCauseOrSuppressed(ex1, "ex1", RuntimeException.class));
    }

    @Test
    void hasCauseOrSuppressedWithCyclicDependencyInSuppressed() {
        var ex0 = new Exception();
        var ex1 = new RuntimeException(ex0);

        ex0.addSuppressed(ex1);

        assertFalse(hasCauseOrSuppressed(ex0, AssertionError.class));
        assertFalse(hasCauseOrSuppressed(ex1, AssertionError.class));

        assertTrue(hasCauseOrSuppressed(ex0, Exception.class));
        assertTrue(hasCauseOrSuppressed(ex0, RuntimeException.class));

        assertTrue(hasCauseOrSuppressed(ex1, Exception.class));
        assertTrue(hasCauseOrSuppressed(ex1, RuntimeException.class));
    }

    @Test
    void hasCauseWithoutMessage() {
        var ex0 = new Exception();
        var ex1 = new RuntimeException(ex0);

        assertFalse(hasCause(ex0, AssertionError.class));
        assertFalse(hasCause(ex1, AssertionError.class));

        assertTrue(hasCause(ex0, Exception.class));
        assertFalse(hasCause(ex0, RuntimeException.class));

        assertTrue(hasCause(ex1, Exception.class));
        assertTrue(hasCause(ex1, RuntimeException.class));
    }

    @Test
    void hasCauseWithMessage() {
        var ex0 = new Exception("ex0");
        var ex1 = new RuntimeException("ex1", ex0);

        assertFalse(hasCause(ex0, AssertionError.class));
        assertFalse(hasCause(ex1, AssertionError.class));

        assertFalse(hasCause(ex0, "foo", Exception.class));
        assertFalse(hasCause(ex1, "bar", RuntimeException.class));

        assertTrue(hasCause(ex0, "e", Exception.class));
        assertTrue(hasCause(ex0, "ex", Exception.class));
        assertTrue(hasCause(ex0, "ex0", Exception.class));

        assertTrue(hasCause(ex1, "ex", Exception.class));
        assertTrue(hasCause(ex1, "e", RuntimeException.class));
        assertTrue(hasCause(ex1, "ex", RuntimeException.class));
        assertTrue(hasCause(ex1, "ex1", RuntimeException.class));
    }

    @Test
    void hasCauseWithCyclicDependency() {
        var ex0 = new Exception();
        var ex1 = new RuntimeException(ex0);

        ex0.initCause(ex1);

        assertFalse(hasCause(ex0, AssertionError.class));
        assertFalse(hasCause(ex1, AssertionError.class));

        assertTrue(hasCause(ex0, Exception.class));
        assertTrue(hasCause(ex0, RuntimeException.class));

        assertTrue(hasCause(ex1, Exception.class));
        assertTrue(hasCause(ex1, RuntimeException.class));
    }

    @Test
    void hasCauseWithoutMessageIgnoresSuppressed() {
        var ex0 = new IOException();
        var ex1 = new RuntimeException();

        ex1.addSuppressed(ex0);

        assertFalse(hasCause(ex1, IOException.class));
    }

    @Test
    void hasCauseWithMessageIgnoresSuppressed() {
        var ex0 = new IOException("Suppressed");
        var ex1 = new RuntimeException();

        ex1.addSuppressed(ex0);

        assertFalse(hasCause(ex1, "Suppressed", IOException.class));
    }

    /** Test exception class. */
    public static class TestException extends IgniteException {
        public TestException() {
            super(NODE_STOPPING_ERR);
        }
    }

    /** Test exception class. */
    public static class TestExceptionWithCode extends IgniteException {
        public TestExceptionWithCode(int code) {
            super(code);
        }
    }

    /** Test exception class. */
    public static class TestExceptionWithMessage extends IgniteException {
        public TestExceptionWithMessage(String message) {
            super(NODE_STOPPING_ERR, message);
        }
    }

    /** Test exception class. */
    public static class TestExceptionWithCause extends IgniteException {
        public TestExceptionWithCause(Throwable t) {
            super(NODE_STOPPING_ERR, t);
        }
    }

    /** Test exception class. */
    public static class TestExceptionWithCodeAndMessage extends IgniteException {
        public TestExceptionWithCodeAndMessage(int code, String message) {
            super(code, message);
        }
    }

    /** Test exception class. */
    public static class TestExceptionWithCodeAndCause extends IgniteException {
        public TestExceptionWithCodeAndCause(int code, Throwable cause) {
            super(code, cause);
        }
    }

    /** Test exception class. */
    public static class TestExceptionWithCodeMessageAndCause extends IgniteException {
        public TestExceptionWithCodeMessageAndCause(int code, String message, Throwable cause) {
            super(code, message, cause);
        }
    }

    /** Test exception class. */
    public static class TestCheckedException extends IgniteCheckedException {
        public TestCheckedException() {
            super(NODE_STOPPING_ERR);
        }
    }

    /** Test exception class. */
    public static class TestCheckedExceptionWithCode extends IgniteCheckedException {
        public TestCheckedExceptionWithCode(int code) {
            super(code);
        }
    }

    /** Test exception class. */
    public static class TestCheckedExceptionWithMessage extends IgniteCheckedException {
        public TestCheckedExceptionWithMessage(String message) {
            super(NODE_STOPPING_ERR, message);
        }
    }

    /** Test exception class. */
    public static class TestCheckedExceptionWithCause extends IgniteCheckedException {
        public TestCheckedExceptionWithCause(Throwable t) {
            super(NODE_STOPPING_ERR, t);
        }
    }

    /** Test exception class. */
    public static class TestCheckedExceptionWithCodeAndMessage extends IgniteCheckedException {
        public TestCheckedExceptionWithCodeAndMessage(int code, String message) {
            super(code, message);
        }
    }

    /** Test exception class. */
    public static class TestCheckedExceptionWithCodeAndCause extends IgniteCheckedException {
        public TestCheckedExceptionWithCodeAndCause(int code, Throwable cause) {
            super(code, cause);
        }
    }

    /** Test exception class. */
    public static class TestCheckedExceptionWithCodeMessageAndCause extends IgniteCheckedException {
        public TestCheckedExceptionWithCodeMessageAndCause(int code, String message, Throwable cause) {
            super(code, message, cause);
        }
    }

    /** Test exception class. */
    public static class TestUncheckedExceptionWithTraceCodeAndCause extends IgniteException {
        public TestUncheckedExceptionWithTraceCodeAndCause(UUID traceId, int code, Throwable cause) {
            super(traceId, code, cause);
        }
    }
}
