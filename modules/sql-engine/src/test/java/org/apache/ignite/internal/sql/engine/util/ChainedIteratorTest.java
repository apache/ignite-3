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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify {@link ChainedIterator}.
 */
@SuppressWarnings("ThrowableNotThrown")
class ChainedIteratorTest {
    @Test
    void emptyIterator() {
        {
            Iterator<Integer> it = new ChainedIterator<>(
                    Collections.emptyIterator()
            );

            assertThat(it.hasNext(), is(false));
            assertThrows(
                    NoSuchElementException.class,
                    it::next,
                    null
            );
        }

        {
            Iterator<Integer> it = new ChainedIterator<>(
                    Collections.emptyIterator(),
                    Collections.emptyIterator()
            );

            assertThat(it.hasNext(), is(false));
            assertThrows(
                    NoSuchElementException.class,
                    it::next,
                    null
            );
        }
    }

    @Test
    void firstIsEmptyAndSecondIsNot() {
        Iterator<Integer> it = new ChainedIterator<>(
                Collections.emptyIterator(),
                List.of(1, 2).iterator()
        );

        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(1));
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(2));
        assertThat(it.hasNext(), is(false));
    }

    @Test
    void firstIsNotEmptyAndSecondIs() {
        Iterator<Integer> it = new ChainedIterator<>(
                List.of(1, 2).iterator(),
                Collections.emptyIterator()
        );

        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(1));
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(2));
        assertThat(it.hasNext(), is(false));
    }

    @Test
    void bothNotEmpty() {
        Iterator<Integer> it = new ChainedIterator<>(
                List.of(1, 2).iterator(),
                List.of(3, 4).iterator()
        );

        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(1));
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(2));
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(3));
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(4));
        assertThat(it.hasNext(), is(false));
    }

    @Test
    void emptyInTheMiddle() {
        Iterator<Integer> it = new ChainedIterator<>(
                List.of(1, 2).iterator(),
                Collections.emptyIterator(),
                Collections.emptyIterator(),
                List.of(3, 4).iterator()
        );

        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(1));
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(2));
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(3));
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is(4));
        assertThat(it.hasNext(), is(false));
    }
}
