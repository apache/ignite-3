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

package org.apache.ignite.internal.app;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.function.LongSupplier;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SameValueLongSupplierTest extends BaseIgniteAbstractTest {
    @Mock
    private LongSupplier supplier;

    @InjectMocks
    private SameValueLongSupplier sameValueSupplier;

    @Test
    void suppliesSameValueSuccessfully() {
        when(supplier.getAsLong()).thenReturn(1L, 1L, 1L);

        assertThat(sameValueSupplier.getAsLong(), is(1L));
        assertThat(sameValueSupplier.getAsLong(), is(1L));
        assertThat(sameValueSupplier.getAsLong(), is(1L));
    }

    @Test
    void failsAssertionOnMismatchingValue() {
        when(supplier.getAsLong()).thenReturn(2L, 3L);

        sameValueSupplier.getAsLong();

        AssertionError error = assertThrows(AssertionError.class, () -> sameValueSupplier.getAsLong());
        assertThat(error.getMessage(), is("Previous value was 2, but current value is 3"));
    }
}
