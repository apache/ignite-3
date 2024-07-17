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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class CursorTest extends BaseIgniteAbstractTest {
    @Test
    void bareIteratorAdapterProvidesDataFromIterator() throws Exception {
        try (Cursor<Integer> cursor = Cursor.fromBareIterator(List.of(1, 2).iterator())) {
            assertThat(cursor.next(), is(1));
            assertThat(cursor.next(), is(2));
        }
    }

    @Test
    void fromBareIteratorThrowsOnCloseableIterator() {
        Exception ex = assertThrows(IllegalArgumentException.class, () -> Cursor.fromBareIterator(mock(CloseableIterator.class)));

        assertThat(ex.getMessage(), endsWith(" implements AutoCloseable, while Cursor#fromBareIterator() "
                + "only supports non-closeable iterators. Please refer to Cursor#fromCloseableIterator()."));
    }

    @Test
    void closeableIteratorAdapterProvidesDataFromIterator() throws Exception {
        CloseableIterator iterator = mock(CloseableIterator.class);

        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(1, 2);

        try (Cursor<Integer> cursor = Cursor.fromCloseableIterator(iterator)) {
            assertThat(cursor.next(), is(1));
            assertThat(cursor.next(), is(2));
        }
    }

    @Test
    void closeableIteratorAdapterClosesIteratorOnClose() throws Exception {
        CloseableIterator iterator = mock(CloseableIterator.class);
        Cursor<?> cursor = Cursor.fromCloseableIterator(iterator);

        cursor.close();

        verify(iterator).close();
    }

    private interface CloseableIterator extends Iterator<Integer>, AutoCloseable {
        @Override
        void close();
    }
}
