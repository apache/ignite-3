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

package org.apache.ignite.lang;

import static org.apache.ignite.lang.ErrorGroup.errorMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.junit.jupiter.api.Test;

/**
 * Tests ignite exceptions.
 */
public class IgniteExceptionTest {
    @Test
    public void testWrapPublicUncheckedException() {
        var originalTraceId = UUID.randomUUID();
        var expectedMessage = errorMessage(originalTraceId, Table.TABLE_NOT_FOUND_ERR, "Error foo bar");

        var originalEx = new CustomTestException(originalTraceId, Table.TABLE_NOT_FOUND_ERR, "Error foo bar", null);
        var wrappedEx = new CompletionException(originalEx);
        var res = IgniteException.wrap(wrappedEx);

        assertEquals(originalEx.traceId(), res.traceId());
        assertEquals(originalEx.code(), res.code());
        assertEquals(originalEx.getClass(), res.getClass());
        assertSame(originalEx, res.getCause());
        assertEquals(expectedMessage, res.getMessage());
    }

    @Test
    public void testWrapPublicCheckedException() {
        var originalTraceId = UUID.randomUUID();
        var expectedMessage = errorMessage(originalTraceId, Table.COLUMN_ALREADY_EXISTS_ERR, "Msg.");

        var originalEx = new IgniteCheckedException(originalTraceId, Table.COLUMN_ALREADY_EXISTS_ERR, "Msg.");
        var wrappedEx = new CompletionException(originalEx);
        var res = IgniteException.wrap(wrappedEx);

        assertEquals(originalEx.traceId(), res.traceId());
        assertEquals(originalEx.code(), res.code());
        assertSame(originalEx, res.getCause());
        assertEquals(expectedMessage, res.getMessage());
    }

    @Test
    public void testWrapInternalException() {
        var originalEx = new IgniteInternalException(UUID.randomUUID(), Common.INTERNAL_ERR, "Unexpected error.");
        var wrappedEx = new CompletionException(originalEx);
        var res = IgniteException.wrap(wrappedEx);

        assertEquals(Common.INTERNAL_ERR, res.code());
    }

    @Test
    public void testWrapInternalCheckedException() {
        var originalEx = new IgniteInternalCheckedException(UUID.randomUUID(), Common.INTERNAL_ERR, "Unexpected error.");
        var wrappedEx = new CompletionException(originalEx);
        var res = IgniteException.wrap(wrappedEx);

        assertEquals(Common.INTERNAL_ERR, res.code());
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
