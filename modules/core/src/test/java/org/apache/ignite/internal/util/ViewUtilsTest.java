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

package org.apache.ignite.internal.util;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.UUID;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

class ViewUtilsTest {

    @Test
    void ensurePublicException() {
        UUID traceId = UUID.randomUUID();
        IgniteException exception = new IgniteException(traceId, INTERNAL_ERR, "message", new RuntimeException("cause"));
        Throwable publicException = ViewUtils.ensurePublicException(exception);

        assertThat(publicException.getMessage(), is("message"));
        assertThat(publicException.getCause(), instanceOf(RuntimeException.class));
        assertThat(publicException.getCause().getMessage(), is("cause"));

        assertThat(publicException, instanceOf(IgniteException.class));
        IgniteException igniteException = (IgniteException) publicException;
        assertThat(igniteException.traceId(), is(traceId));
        assertThat(igniteException.code(), is(INTERNAL_ERR));
    }
}
