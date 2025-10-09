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

import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class ExceptionUtilsTest {
    /**
     * Tests a particular case when an {@link ExceptionInInitializerError} gets copied. This error does not have a
     * {@link Throwable#Throwable(String, Throwable)} constructor and we at least expect to not lose the original stacktrace.
     */
    @Test
    void testCopyExceptionInInitializerError() {
        var exception = new ExecutionException(new ExceptionInInitializerError(new IllegalArgumentException()));

        Throwable copied = ExceptionUtils.copyExceptionWithCause(exception);

        assertThat(copied, isA(ExceptionInInitializerError.class));
        assertThat(hasCause(copied, IllegalArgumentException.class), is(true));
    }
}
