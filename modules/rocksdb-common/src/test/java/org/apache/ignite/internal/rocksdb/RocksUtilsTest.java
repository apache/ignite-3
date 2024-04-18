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

package org.apache.ignite.internal.rocksdb;

import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.AbstractNativeReference;

@ExtendWith(MockitoExtension.class)
class RocksUtilsTest extends BaseIgniteAbstractTest {
    @Mock
    private AbstractNativeReference ref1;

    @Mock
    private AbstractNativeReference ref2;

    @ParameterizedTest
    @EnumSource(CloseAll.class)
    void closeAllClosesAll(CloseAll action) {
        action.closeAll(ref1, ref2);

        verify(ref1).close();
        verify(ref2).close();
    }

    @ParameterizedTest
    @EnumSource(CloseAll.class)
    void closeAllClosesEvenIfExceptionHappens(CloseAll action) {
        doThrow(new RuntimeException("Oops")).when(ref1).close();

        assertThrows(RuntimeException.class, () -> action.closeAll(ref1, ref2));

        verify(ref1).close();
        verify(ref2).close();
    }

    @ParameterizedTest
    @EnumSource(CloseAll.class)
    void firstExceptionFromCloseAllIsThrown(CloseAll action) {
        RuntimeException cause = new RuntimeException("Oops");
        doThrow(cause).when(ref1).close();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> action.closeAll(ref1, ref2));

        assertThat(ex, is(cause));
    }

    @ParameterizedTest
    @EnumSource(CloseAll.class)
    void secondExceptionFromCloseAllIsAddedToSuppressed(CloseAll action) {
        RuntimeException cause = new RuntimeException("Oops");
        doThrow(cause).when(ref1).close();

        RuntimeException cause2 = new RuntimeException("Oops2");
        doThrow(cause2).when(ref2).close();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> action.closeAll(ref1, ref2));

        assertThat(ex.getSuppressed().length, is(1));
        assertThat(ex.getSuppressed()[0], is(cause2));
    }

    @ParameterizedTest
    @EnumSource(CloseAll.class)
    void closeAllToleratesNulls(CloseAll action) {
        action.closeAll(null, ref2);

        verify(ref2).close();
    }

    private enum CloseAll {
        ARRAY {
            @Override
            void closeAll(@Nullable AbstractNativeReference ref1, @Nullable AbstractNativeReference ref2) {
                RocksUtils.closeAll(ref1, ref2);
            }
        },
        COLLECTION {
            @Override
            void closeAll(@Nullable AbstractNativeReference ref1, @Nullable AbstractNativeReference ref2) {
                RocksUtils.closeAll(Arrays.asList(ref1, ref2));
            }
        };

        abstract void closeAll(@Nullable AbstractNativeReference ref1, @Nullable AbstractNativeReference ref2);
    }

    @Test
    void testIncrementPrefix() {
        byte[] incremented = incrementPrefix(new byte[] {0, 1, Byte.MAX_VALUE});

        assertThat(incremented, is(new byte[] {0, 1, Byte.MIN_VALUE}));

        incremented = incrementPrefix(new byte[] {0, 1, -1});

        assertThat(incremented, is(new byte[] {0, 2}));

        incremented = incrementPrefix(new byte[] {0, -1, -1});

        assertThat(incremented, is(new byte[] {1}));

        incremented = incrementPrefix(new byte[] {-1, -1, -1});

        assertThat(incremented, is(nullValue()));
    }
}
