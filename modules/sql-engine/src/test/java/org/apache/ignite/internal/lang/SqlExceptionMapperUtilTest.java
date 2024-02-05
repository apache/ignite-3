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

import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.stream.Stream;
import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests mapping internal exceptions to public SqlException.
 */
class SqlExceptionMapperUtilTest {
    /**
     * Tests a default mapping of internal exceptions passed from the sql engine.
     */
    @Test
    public void testSqlInternalExceptionDefaultMapping() {
        CustomNoMappingException internalSqlErr = new CustomNoMappingException(EXECUTION_CANCELLED_ERR);
        Throwable mappedErr = mapToPublicSqlException(internalSqlErr);

        assertThat(mappedErr, instanceOf(SqlException.class));

        SqlException mappedSqlErr = (SqlException) mappedErr;

        assertThat("Mapped exception should have the same trace identifier.", mappedSqlErr.traceId(), is(internalSqlErr.traceId()));
        assertThat("Mapped exception shouldn't have the same error code.", mappedSqlErr.code(), is(INTERNAL_ERR));
    }

    private static Stream<Arguments> testSqlInternalExceptionDefaultMappingForPublicException() {
        return Stream.of(
                Arguments.of(new NoRowSetExpectedException()),
                Arguments.of(new CursorClosedException())
        );
    }

    /**
     * Tests a default mapping of internal exceptions passed from the sql engine.
     */
    @ParameterizedTest
    @MethodSource
    public void testSqlInternalExceptionDefaultMappingForPublicException(Throwable err) {
        Throwable mappedErr = mapToPublicSqlException(err);

        assertSame(err, mappedErr);
    }

    /**
     * Test exception.
     */
    public static class CustomNoMappingException extends IgniteInternalException {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /**
         * Creates a new instance of CustomNoMappingException with given code.
         */
        public CustomNoMappingException(int code) {
            super(code, "Test internal exception [err=no mapping]");
        }
    }
}
