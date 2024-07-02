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

package org.apache.ignite.internal.client;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Future utils test.
 */
public class ClientUtilsTest {
    @Test
    public void testEnsurePublicExceptionIgniteException() {
        IgniteException ex = assertThrows(IgniteException.class, ClientUtilsTest::throwIgniteException);

        Throwable resEx = checkableTestMethod(ex);

        assertTrue(resEx instanceof IgniteException);
        assertEquals("Test ignite exception", resEx.getMessage());
        assertEquals(ex.getMessage(), resEx.getMessage());
        assertThat(Arrays.asList(ex.getStackTrace()), anyOf(hasToString(containsString("throwIgniteException"))));
        assertThat(Arrays.asList(resEx.getStackTrace()), anyOf(hasToString(containsString("checkableTestMethod"))));
        assertSame(ex.getClass(), resEx.getCause().getClass());
    }

    @Test
    public void testEnsurePublicExceptionIgniteCheckedException() {
        IgniteCheckedException ex = assertThrows(IgniteCheckedException.class, ClientUtilsTest::throwIgniteCheckedException);

        Throwable resEx = checkableTestMethod(ex);

        assertTrue(resEx instanceof IgniteCheckedException);
        assertEquals("Test checked exception", resEx.getMessage());
        assertEquals(ex.getMessage(), resEx.getMessage());
        assertThat(Arrays.asList(ex.getStackTrace()), anyOf(hasToString(containsString("throwIgniteCheckedException"))));
        assertThat(Arrays.asList(resEx.getStackTrace()), anyOf(hasToString(containsString("checkableTestMethod"))));
        assertSame(ex.getClass(), resEx.getCause().getClass());
    }

    @Test
    public void testEnsurePublicExceptionRuntimeException() {
        RuntimeException ex = assertThrows(RuntimeException.class, ClientUtilsTest::throwRuntimeException);

        Throwable resEx = checkableTestMethod(ex);

        assertTrue(resEx instanceof IgniteException);
        assertEquals("Test runtime exception", resEx.getMessage());
        assertEquals(ex.getMessage(), resEx.getMessage());
        assertThat(Arrays.asList(ex.getStackTrace()), anyOf(hasToString(containsString("throwRuntimeException"))));
        assertThat(Arrays.asList(resEx.getStackTrace()), anyOf(hasToString(containsString("checkableTestMethod"))));
        assertSame(IgniteException.class, resEx.getCause().getClass());
        assertSame(ex.getClass(), resEx.getCause().getCause().getClass());
    }

    @Test
    public void testEnsurePublicExceptionInvalidIgniteException() {
        InvalidIgniteException ex = assertThrows(InvalidIgniteException.class, ClientUtilsTest::throwInvalidIgniteException);

        Throwable resEx = checkableTestMethod(ex);

        assertTrue(resEx instanceof IgniteException);
        assertThat(resEx.getMessage(), containsString("Public Ignite exception-derived class does not have required constructor"));
        assertThat(Arrays.asList(ex.getStackTrace()), anyOf(hasToString(containsString("throwInvalidIgniteException"))));
        assertThat(Arrays.asList(resEx.getStackTrace()), anyOf(hasToString(containsString("checkableTestMethod"))));
        assertSame(InvalidIgniteException.class, resEx.getCause().getClass());
        assertSame(ex.getClass(), resEx.getCause().getClass());
    }

    /**
     * Method that should present in resulting stack trace.
     */
    private static Throwable checkableTestMethod(Throwable ex) {
        return ClientUtils.ensurePublicException(ex);
    }

    /**
     * Un-constructable IgniteException.
     */
    private static class InvalidIgniteException extends IgniteException {
        InvalidIgniteException(int code, String message, @Nullable Throwable cause) {
            super(code, message, cause);
        }
    }

    private static void throwInvalidIgniteException() {
        throw new InvalidIgniteException(INTERNAL_ERR, "Test invalid ignite exception", null);
    }

    private static void throwIgniteException() {
        throw new IgniteException(INTERNAL_ERR, "Test ignite exception", null);
    }

    private static void throwIgniteCheckedException() throws IgniteCheckedException {
        throw new IgniteCheckedException(INTERNAL_ERR, "Test checked exception", null);
    }

    private static void throwRuntimeException() {
        throw new RuntimeException("Test runtime exception", null);
    }
}
