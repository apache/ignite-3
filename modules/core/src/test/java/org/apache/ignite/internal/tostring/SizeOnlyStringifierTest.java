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

package org.apache.ignite.internal.tostring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** For {@link SizeOnlyStringifier} testing. */
public class SizeOnlyStringifierTest {
    private final SizeOnlyStringifier stringifier = new SizeOnlyStringifier();

    @Test
    void testCollection() {
        assertEquals("0", stringifier.toString(List.of()));
        assertEquals("1", stringifier.toString(Set.of(1)));
    }

    @Test
    void testMap() {
        assertEquals("0", stringifier.toString(Map.of()));
        assertEquals("1", stringifier.toString(Map.of(1, "1")));
    }

    @Test
    void testArray() {
        assertEquals("0", stringifier.toString(new byte[]{}));
        assertEquals("1", stringifier.toString(new boolean[]{true}));
        assertEquals("1", stringifier.toString(new short[]{1}));
        assertEquals("1", stringifier.toString(new char[]{1}));
        assertEquals("1", stringifier.toString(new int[]{1}));
        assertEquals("1", stringifier.toString(new long[]{1}));
        assertEquals("1", stringifier.toString(new float[]{1}));
        assertEquals("1", stringifier.toString(new double[]{1}));
        assertEquals("2", stringifier.toString(new Object[]{1, null}));
        assertEquals("3", stringifier.toString(new String[]{"1", "2", "3"}));
    }

    @Test
    void testUnexpectedType() {
        assertThrows(IllegalArgumentException.class, () -> stringifier.toString(1));
        assertThrows(IllegalArgumentException.class, () -> stringifier.toString(""));
        assertThrows(IllegalArgumentException.class, () -> stringifier.toString(4L));
    }
}
