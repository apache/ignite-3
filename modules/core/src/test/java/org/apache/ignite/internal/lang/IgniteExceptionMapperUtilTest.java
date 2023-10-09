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

import static org.apache.ignite.internal.lang.IgniteExceptionMapper.checked;
import static org.apache.ignite.internal.lang.IgniteExceptionMapper.unchecked;
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.registerMapping;
import static org.apache.ignite.lang.ErrorGroups.Common.COMMON_ERR_GROUP;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

/**
 * Tests mapping internal exceptions to public ones.
 */
public class IgniteExceptionMapperUtilTest {
    /** Internal collection of mappers for tests. */
    private Map<Class<? extends Exception>, IgniteExceptionMapper<?, ?>> mappers = new HashMap<>();

    /**
     * Tests a simple scenario of registering mapper for internal exceptions.
     */
    @Test
    public void testRegisterBasicMapper() {
        IgniteExceptionMapper<CustomInternalException, IgniteException> m1 =
                unchecked(CustomInternalException.class, err -> new IgniteException(NODE_STOPPING_ERR, "test"));
        IgniteExceptionMapper<CustomInternalCheckedException, IgniteCheckedException> m2 =
                checked(CustomInternalCheckedException.class, err -> new IgniteCheckedException(NODE_STOPPING_ERR, "test"));

        registerMapping(m1, mappers);
        registerMapping(m2, mappers);

        assertThat("Failed to register both exception mappers.", mappers.size(), is(2));
    }

    /**
     * Tests that the only one mapper can be registered for a class.
     */
    @Test
    public void testRegisterDuplicateMapper() {
        IgniteExceptionMapper<CustomInternalException, IgniteException> m =
                unchecked(CustomInternalException.class, err -> new IgniteException(NODE_STOPPING_ERR, "test"));

        registerMapping(m, mappers);

        IgniteException e = assertThrows(IgniteException.class, () -> registerMapping(m, mappers));

        assertThat("Unexpected error group code, it should be COMMON_ERR_GROUP.", e.groupCode(), is(COMMON_ERR_GROUP.groupCode()));
        assertThat("Unexpected error code, it should be INTERNAL_ERR.", e.code(), is(INTERNAL_ERR));
    }

    /**
     * Tests that {@link AssertionError} is mapped to {@link IgniteException} with the {@link Common#INTERNAL_ERR}.
     */
    @Test
    public void testAssertionErrorMapping() {
        Throwable t = mapToPublicException(new AssertionError("test assertion"));

        assertThat("AssertionError should be mapped to IgniteException", t, isA(IgniteException.class));

        IgniteException mapped = (IgniteException) t;

        assertThat("Unexpected error group code, it should be COMMON_ERR_GROUP.", mapped.groupCode(), is(COMMON_ERR_GROUP.groupCode()));
        assertThat("Unexpected error code, it should be INTERNAL_ERR.", mapped.code(), is(INTERNAL_ERR));
    }

    /**
     * Tests that {@link AssertionError} is mapped to {@link IgniteException} with the {@link Common#INTERNAL_ERR}.
     */
    @Test
    public void testErrorMapping() {
        Throwable t = mapToPublicException(new OutOfMemoryError("test error"));

        assertThat("Error should not be mapped to any exception.", t, isA(Error.class));
    }

    /**
     * Tests a mapping from Ignite internal exception to a public one.
     */
    @Test
    public void testInternalExceptionMapping() {
        CustomInternalException internalErr = new CustomInternalException();

        Throwable mappedErr = mapToPublicException(internalErr);

        assertThat("Mapped exception should be an instance of IgniteException.", mappedErr, isA(IgniteException.class));

        IgniteException mappedPublicErr = (IgniteException) mappedErr;

        assertThat("Mapped exception should have the same trace identifier.", mappedPublicErr.traceId(), is(internalErr.traceId()));
    }

    /**
     * Tests a mapping from Ignite internal exception to a public one.
     */
    @Test
    public void testInternalCheckedExceptionMapping() {
        CustomInternalCheckedException internalErr = new CustomInternalCheckedException();

        Throwable mappedErr = mapToPublicException(internalErr);

        assertThat("Mapped exception should be an instance of IgniteException.", mappedErr, isA(IgniteCheckedException.class));

        IgniteCheckedException mappedPublicErr = (IgniteCheckedException) mappedErr;

        assertThat("Mapped exception should have the same trace identifier.", mappedPublicErr.traceId(), is(internalErr.traceId()));
    }

    /**
     * Tests that mapping of unregistered type of exception results in {@link IgniteException}
     * with the {@link Common#INTERNAL_ERR}.
     */
    @Test
    public void testExceptionMappingOfUnregisteredType() {
        CustomNoMappingException err = new CustomNoMappingException();

        Throwable t = mapToPublicException(err);

        assertThat("Unregistered exception should be mapped to IgniteException", t, isA(IgniteException.class));

        IgniteException mapped = (IgniteException) t;

        assertThat("Unexpected error group code, it should be COMMON_ERR_GROUP.", mapped.groupCode(), is(COMMON_ERR_GROUP.groupCode()));
        assertThat("Unexpected error code, it should be INTERNAL_ERR.", mapped.code(), is(INTERNAL_ERR));
    }

    /**
     * Test runtime exception.
     */
    public static class CustomInternalException extends IgniteInternalException {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /**
         * Creates a new instance of CustomInternalException.
         */
        public CustomInternalException() {
            super(INTERNAL_ERR, "Test internal exception.");
        }
    }

    /**
     * Test checked exception.
     */
    public static class CustomInternalCheckedException extends IgniteInternalCheckedException {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /**
         * Creates a new instance of CustomInternalCheckedException.
         */
        public CustomInternalCheckedException() {
            super(INTERNAL_ERR, "Test internal checked exception.");
        }

        /**
         * Creates a new instance of CustomInternalCheckedException.
         *
         * @param traceId Trace identifier.
         */
        public CustomInternalCheckedException(UUID traceId) {
            super(traceId, INTERNAL_ERR, "Test internal checked exception.");
        }
    }

    /**
     * Test exception.
     */
    public static class CustomNoMappingException extends IgniteInternalException {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /**
         * Creates a new instance of CustomNoMappingException.
         */
        public CustomNoMappingException() {
            super(INTERNAL_ERR, "Test internal exception [err=no mapping]");
        }
    }
}
