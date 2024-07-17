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

import static org.apache.ignite.internal.util.CursorUtils.map;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Class for testing {@link CursorUtils}.
 */
public class CursorUtilsTest {
    @Test
    public void testMap() {
        Cursor<String> actual = map(cursor(1, 2, 5, 14, 20), i -> "foo" + i);

        assertThat(actual, contains("foo1", "foo2", "foo5", "foo14", "foo20"));

        assertThat(map(cursor(), Object::toString), is(emptyIterable()));
    }

    @SafeVarargs
    private static <T> Cursor<T> cursor(T... elements) {
        return Cursor.fromBareIterator(Arrays.asList(elements).iterator());
    }
}
