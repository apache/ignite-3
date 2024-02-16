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

import static org.apache.ignite.lang.ErrorGroups.extractGroupCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;

/**
 * Test utils for checking public exceptions.
 */
public class IgniteExceptionTestUtils {
    /**
     * <em>Assert</em> that passed throwable is a public exception with expected error group, code and message.
     *
     * @param throwable exception to check.
     * @param expectedErrorCode - expected error code.
     * @param containMessage - message that exception should contain.
     */
    public static void assertPublicException(
            Throwable throwable,
            int expectedErrorCode,
            String containMessage
    ) {
        assertTraceableException(throwable, IgniteException.class, expectedErrorCode, containMessage);
    }

    /**
     * <em>Assert</em> that passed throwable is a public checked exception with expected error group, code and message.
     *
     * @param throwable exception to check.
     * @param expectedErrorCode - expected error code.
     * @param containMessage - message that exception should contain.
     */
    public static void assertPublicCheckedException(
            Throwable throwable,
            int expectedErrorCode,
            String containMessage
    ) {
        assertTraceableException(throwable, IgniteCheckedException.class, expectedErrorCode, containMessage);
    }

    /**
     * <em>Assert</em> that passed throwable is a traceable exception with expected type, error group, code and message.
     *
     * @param throwable - exception to check.
     * @param expectedType - expected public exception type.
     * @param expectedErrorCode - expected error code.
     * @param containMessage - message that exception should contain.
     */
    public static void assertTraceableException(
            Throwable throwable,
            Class<? extends TraceableException> expectedType,
            int expectedErrorCode,
            String containMessage
    ) {
        Throwable cause = ExceptionUtils.unwrapCause(throwable);

        assertThat(cause, instanceOf(expectedType));
        TraceableException ex = expectedType.cast(cause);

        assertThat(ex.groupCode(), is(extractGroupCode(expectedErrorCode)));
        assertThat(ex.code(), is(expectedErrorCode));
        assertThat(ex.traceId(), is(notNullValue()));
        assertThat(cause.getMessage(), containsString(containMessage));
    }
}
