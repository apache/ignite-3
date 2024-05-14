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

package org.apache.ignite.internal.testframework;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.util.function.Supplier;
import org.mockito.Answers;
import org.mockito.invocation.InvocationOnMock;

/** Some utilities for tests that use Mockito. */
public class MockitoTestUtils {
    private MockitoTestUtils() {
        // Intentionally left blank.
    }

    /**
     * Creates a mockito stub only spy. Similar to {@link org.mockito.Mockito#spy(Object)} but does not track invocations.
     * Useful to add append functionality to existing methods.
     *
     * @param <T> The type of the spied object.
     * @param spiedInstanceSupplier Supplier for the spied instance. Removes the need for temporary variables.
     * @return The created mockito spy.
     */
    public static <T> T spyStubOnly(Supplier<T> spiedInstanceSupplier) {
        T instance = spiedInstanceSupplier.get();
        return (T) mock(
                instance.getClass(),
                withSettings().spiedInstance(instance).defaultAnswer(Answers.CALLS_REAL_METHODS).stubOnly()
        );
    }

    /**
     * Tries calling the real method on a mock invocation, wrapping any error into a RuntimeException.
     *
     * @param <T> The expected type of the response.
     * @param invocationOnMock The mockito invocation object.
     * @return The return of the real method.
     */
    public static <T> T tryCallRealMethod(InvocationOnMock invocationOnMock) {
        try {
            return (T) invocationOnMock.callRealMethod();
        } catch (Throwable e) {
            throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
        }
    }
}
