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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.internal.table.criteria.CriteriaExceptionMapperUtil.mapToPublicCriteriaException;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.table.criteria.CriteriaException;
import org.junit.jupiter.api.Test;

/**
 * Tests mapping internal exceptions to public {@link CriteriaException}.
 */
class CriteriaExceptionMapperUtilTest {
    /**
     * Tests a default mapping of internal exceptions passed from the sql engine.
     */
    @Test
    public void testMappingForInternalException() {
        IgniteInternalException internalErr = new IgniteInternalException(EXECUTION_CANCELLED_ERR);
        Throwable mappedErr = mapToPublicCriteriaException(internalErr);

        assertThat(mappedErr, instanceOf(CriteriaException.class));

        CriteriaException mappedCriteriaErr = (CriteriaException) mappedErr;

        assertThat("Mapped exception should have the same trace identifier.", mappedCriteriaErr.traceId(), is(internalErr.traceId()));
        assertThat("Mapped exception shouldn't have the same error code.", mappedCriteriaErr.code(), is(INTERNAL_ERR));
    }

    /**
     * Tests a default mapping of exceptions passed from the sql engine.
     */
    @Test
    public void testMappingForSqlException() {
        NoRowSetExpectedException sqlErr = new NoRowSetExpectedException();
        Throwable mappedErr = mapToPublicCriteriaException(sqlErr);

        assertThat(mappedErr, instanceOf(CriteriaException.class));

        CriteriaException mappedCriteriaErr = (CriteriaException) mappedErr;

        assertThat("Mapped exception should have the same trace identifier.", mappedCriteriaErr.traceId(), is(sqlErr.traceId()));
        assertThat("Mapped exception should have the same error code.", mappedCriteriaErr.code(), is(sqlErr.code()));
    }

    /**
     * Tests a default mapping of exceptions.
     */
    @Test
    public void testMappingForCriteriaException() {
        CriteriaException criteriaErr = new CriteriaException(INTERNAL_ERR, new NoRowSetExpectedException());

        Throwable mappedErr = mapToPublicCriteriaException(criteriaErr);

        assertSame(criteriaErr, mappedErr);
    }
}
