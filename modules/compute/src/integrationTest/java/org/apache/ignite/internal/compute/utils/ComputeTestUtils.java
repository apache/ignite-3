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

package org.apache.ignite.internal.compute.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroup;
import org.apache.ignite.lang.IgniteException;
import org.hamcrest.Matchers;

/**
 * Test utils for Compute.
 */
public class ComputeTestUtils {
    /**
     * <em>Assert</em> that passed throwable is a public exception with expected error group, code and message.
     *
     * @param throwable exception to check.
     * @param expectedErrorGroup - expected {@link ErrorGroup}.
     * @param expectedErrorCode - expected error code.
     * @param containMessage - message that exception should contain.
     */
    public static void assertPublicException(
            Throwable throwable,
            ErrorGroup expectedErrorGroup,
            int expectedErrorCode,
            String containMessage
    ) {
        assertPublicException(throwable, IgniteException.class, expectedErrorGroup, expectedErrorCode, containMessage);
    }

    /**
     * <em>Assert</em> that passed throwable is a public exception with expected type, error group, code and message.
     *
     * @param throwable - exception to check.
     * @param expectedType - expected public exception type.
     * @param expectedErrorGroup - expected {@link ErrorGroup}.
     * @param expectedErrorCode - expected error code.
     * @param containMessage - message that exception should contain.
     */
    public static void assertPublicException(
            Throwable throwable,
            Class<? extends IgniteException> expectedType,
            ErrorGroup expectedErrorGroup,
            int expectedErrorCode,
            String containMessage
    ) {
        Throwable cause = ExceptionUtils.unwrapCause(throwable);

        assertThat(cause, instanceOf(expectedType));
        IgniteException ex = expectedType.cast(cause);

        assertThat(ex.groupCode(), is(expectedErrorGroup.groupCode()));
        assertThat(ex.groupName(), is(expectedErrorGroup.name()));
        assertThat(ex.code(), is(expectedErrorCode));
        assertThat(ex.traceId(), is(notNullValue()));
        assertThat(ex.getMessage(), Matchers.containsString(containMessage));
    }
}
