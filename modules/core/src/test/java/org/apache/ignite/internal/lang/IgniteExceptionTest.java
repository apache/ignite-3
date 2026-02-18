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

import static java.lang.invoke.MethodType.methodType;
import static org.apache.ignite.lang.ErrorGroup.errorMessage;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.util.TraceIdUtils.getOrCreateTraceId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests ignite exceptions.
 */
public class IgniteExceptionTest {
    @Test
    public void testWrapPublicUncheckedException() {
        var originalMessage = "Error foo bar";
        var originalTraceId = UUID.randomUUID();
        var expectedFullMessage = CustomTestException.class.getName() + ": "
                + errorMessage(originalTraceId, Table.TABLE_NOT_FOUND_ERR, originalMessage);

        var originalEx = new CustomTestException(originalTraceId, Table.TABLE_NOT_FOUND_ERR, originalMessage, null);
        var wrappedEx = new CompletionException(originalEx);
        IgniteException res = ExceptionUtils.wrap(wrappedEx);

        assertEquals(originalEx.traceId(), res.traceId());
        assertEquals(originalEx.code(), res.code());
        assertEquals(originalEx.getClass(), res.getClass());
        assertSame(originalEx, res.getCause());
        assertEquals(originalMessage, res.getMessage());
        assertEquals(expectedFullMessage, res.toString());
    }

    @Test
    public void testWrapPublicCheckedException() {
        var originalMessage = "Msg";
        var originalTraceId = UUID.randomUUID();
        var expectedFullMessage = IgniteException.class.getName() + ": "
                + errorMessage(originalTraceId, Table.COLUMN_NOT_FOUND_ERR, originalMessage);

        var originalEx = new IgniteCheckedException(originalTraceId, Table.COLUMN_NOT_FOUND_ERR, originalMessage);
        var wrappedEx = new CompletionException(originalEx);
        IgniteException res = ExceptionUtils.wrap(wrappedEx);

        assertEquals(originalEx.traceId(), res.traceId());
        assertEquals(originalEx.code(), res.code());
        assertSame(originalEx, res.getCause());
        assertEquals(originalMessage, res.getMessage());
        assertEquals(expectedFullMessage, res.toString());
    }

    @Test
    public void testWrapInternalException() {
        var originalMessage = "Unexpected error.";
        var originalTraceId = UUID.randomUUID();

        var originalEx = new IgniteInternalException(originalTraceId, INTERNAL_ERR, originalMessage);
        var wrappedEx = new CompletionException(originalEx);
        IgniteException res = ExceptionUtils.wrap(wrappedEx);

        assertEquals(INTERNAL_ERR, res.code());
        assertSame(originalEx, res.getCause());
        assertEquals(originalMessage, res.getMessage());
    }

    @Test
    public void testWrapInternalCheckedException() {
        var originalMessage = "Unexpected error.";
        var originalTraceId = UUID.randomUUID();

        var originalEx = new IgniteInternalCheckedException(originalTraceId, INTERNAL_ERR, originalMessage);
        var wrappedEx = new CompletionException(originalEx);
        IgniteException res = ExceptionUtils.wrap(wrappedEx);

        assertEquals(INTERNAL_ERR, res.code());
        assertSame(originalEx, res.getCause());
        assertEquals(originalMessage, res.getMessage());
    }

    @Test
    public void testDuplicateErrorCode() {
        var originalEx = new CustomTestException(Table.TABLE_NOT_FOUND_ERR, "Error foo bar", null);
        var wrappedEx = new CustomTestException(originalEx.traceId(), originalEx.code(), originalEx.getMessage(), originalEx);

        assertEquals(originalEx.traceId(), wrappedEx.traceId());
        assertEquals(originalEx.code(), wrappedEx.code());
        assertSame(originalEx, wrappedEx.getCause());
        assertEquals(originalEx.getMessage(), wrappedEx.getMessage());
    }

    @ParameterizedTest
    @ValueSource(classes = {
            IgniteException.class,
            IgniteCheckedException.class,
            IgniteInternalException.class,
            IgniteInternalCheckedException.class})
    public void testPropagationTraceIdentifier(Class<? extends Exception> c) {
        class Descriptor {
            private final MethodType signature;
            // TODO https://issues.apache.org/jira/browse/IGNITE-19541
            // This field should removed after fixing the issue.
            private final boolean deprecated;
            private final Object[] args;

            private Descriptor(MethodType signature, boolean deprecated, Object[] args) {
                this.signature = signature;
                this.deprecated = deprecated;
                this.args = args;
            }
        }

        Descriptor[] signatures = {
                new Descriptor(methodType(void.class, Throwable.class), true, new Object[0]),
                new Descriptor(methodType(void.class, String.class, Throwable.class), true, new Object[] {"test-message"}),
                new Descriptor(methodType(void.class, int.class, Throwable.class), false, new Object[] {INTERNAL_ERR}),
                new Descriptor(
                        methodType(void.class, int.class, String.class, Throwable.class),
                        false,
                        new Object[] {INTERNAL_ERR, "test-message"})
        };

        var cause = new IgniteException(INTERNAL_ERR);

        for (var p : signatures) {
            TraceableException err = null;

            try {
                Object[] args = new Object[p.args.length + 1];
                System.arraycopy(p.args, 0, args, 0, p.args.length);
                args[p.args.length] = cause;

                err = (TraceableException) MethodHandles.publicLookup()
                        .findConstructor(c, p.signature)
                        .invokeWithArguments(args);
            } catch (NoSuchMethodException e) {
                if (!p.deprecated) {
                    fail("Failed to find constructor for exception class "
                            + "[class=" + c.getCanonicalName() + ", signature=" + p.signature + ']', e);
                }
                continue;
            } catch (Throwable e) {
                fail("Failed to instantiate a new instance of exception class "
                        + "[class=" + c.getCanonicalName() + ", signature=" + p.signature + ']', e);
            }

            assertThat("Unexpected trace identifier.", err.traceId(), is(cause.traceId()));
        }
    }

    @Test
    public void testExtractionTraceIdentifierFromNonTraceableException() {
        var cause = new RuntimeException(new IllegalArgumentException());

        UUID traceId = getOrCreateTraceId(cause);

        assertThat(traceId, is(any(UUID.class)));
    }

    @Test
    public void testExtractionTraceIdentifierFromNonTraceableExceptionWithCycle() {
        var cause1 = new IllegalArgumentException();
        var cause2 = new RuntimeException(cause1);

        cause1.initCause(cause2);

        UUID traceId = getOrCreateTraceId(cause2);

        assertThat(traceId, is(any(UUID.class)));
    }

    @Test
    public void testIgniteExceptionProperties() {
        var ex = new IgniteException(UUID.randomUUID(), INTERNAL_ERR, "msg");

        assertEquals(INTERNAL_ERR, ex.code());

        assertEquals(-1, ex.errorCode());
        assertEquals("IGN-CMN-65535", ex.codeAsString());

        assertEquals(1, ex.groupCode());
        assertEquals("CMN", ex.groupName());

        assertTrue(ex.toString().contains("IGN-CMN-65535"), ex.toString());
    }

    @Test
    public void testIgniteCheckedExceptionProperties() {
        var ex = new IgniteCheckedException(UUID.randomUUID(), INTERNAL_ERR, "msg");

        assertEquals(INTERNAL_ERR, ex.code());

        assertEquals(-1, ex.errorCode());
        assertEquals("IGN-CMN-65535", ex.codeAsString());

        assertEquals(1, ex.groupCode());
        assertEquals("CMN", ex.groupName());

        assertTrue(ex.toString().contains("IGN-CMN-65535"), ex.toString());
    }

    @Test
    public void testIgniteInternalExceptionProperties() {
        var ex = new IgniteInternalException(UUID.randomUUID(), INTERNAL_ERR, "msg");

        assertEquals(INTERNAL_ERR, ex.code());

        assertEquals(-1, ex.errorCode());
        assertEquals("IGN-CMN-65535", ex.codeAsString());

        assertEquals(1, ex.groupCode());
        assertEquals("CMN", ex.groupName());

        assertTrue(ex.toString().contains("IGN-CMN-65535"), ex.toString());
    }

    @Test
    public void testIgniteInternalCheckedExceptionProperties() {
        var ex = new IgniteInternalCheckedException(UUID.randomUUID(), INTERNAL_ERR, "msg");

        assertEquals(INTERNAL_ERR, ex.code());

        assertEquals(-1, ex.errorCode());
        assertEquals("IGN-CMN-65535", ex.codeAsString());

        assertEquals(1, ex.groupCode());
        assertEquals("CMN", ex.groupName());

        assertTrue(ex.toString().contains("IGN-CMN-65535"), ex.toString());
    }

    @Test
    public void testUnknownErrorCode() {
        int unknownCode = (999 << 16) | 1;
        UUID traceId = UUID.randomUUID();
        String message = "Error from unknown group";

        IgniteException ex = new IgniteException(traceId, unknownCode, message);

        assertEquals(unknownCode, ex.code());
        assertEquals((short) 999, ex.groupCode());
        assertEquals((short) 1, ex.errorCode());
        assertEquals(traceId, ex.traceId());
        assertEquals(message, ex.getMessage());

        assertTrue(ex.toString().contains(message));
    }

    @Test
    public void testUnknownErrorCodeWithCause() {
        int unknownCode = (888 << 16) | 42;
        UUID traceId = UUID.randomUUID();
        String message = "Another error from unknown group";
        Throwable cause = new RuntimeException("Root cause");

        IgniteException ex = new IgniteException(traceId, unknownCode, message, cause);

        assertEquals(unknownCode, ex.code());
        assertEquals(traceId, ex.traceId());
        assertEquals(message, ex.getMessage());
        assertEquals(cause, ex.getCause());
    }

    /**
     * Custom exception for tests.
     */
    public static class CustomTestException extends IgniteException {
        public CustomTestException(int code, String message, Throwable cause) {
            super(code, message, cause);
        }

        public CustomTestException(UUID traceId, int code, String message, Throwable cause) {
            super(traceId, code, message, cause);
        }
    }
}
