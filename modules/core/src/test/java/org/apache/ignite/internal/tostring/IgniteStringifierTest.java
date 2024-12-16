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

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** For {@link IgniteStringifier} testing. */
public class IgniteStringifierTest {
    @Test
    void testFieldsOnly() {
        String exp = "FieldsToStringOnly [nullArray0=null, nullArray1Size=null, list0=1, list1.size=2, int0=foobar, int1.changed=foobar]";

        assertEquals(exp, new FieldsToStringOnly().toString());
    }

    @Test
    void testClassOnly() {
        assertEquals("ClassToStringOnly [foobar]", new ClassToStringOnly().toString());
    }

    @Test
    void testClassOnlyWithChangeName() {
        assertEquals("TestChangeName [foobar]", new ClassToStringOnlyWithChangeName().toString());
    }

    private static class FieldsToStringOnly {
        @IgniteStringifier(SizeOnlyStringifier.class)
        @IgniteToStringOrder(0)
        private int[] nullArray0;

        @IgniteStringifier(name = "nullArray1Size", value = SizeOnlyStringifier.class)
        @IgniteToStringOrder(1)
        private boolean[] nullArray1;

        @IgniteStringifier(SizeOnlyStringifier.class)
        @IgniteToStringOrder(2)
        private final List<Integer> list0 = List.of(0);

        @IgniteStringifier(name = "list1.size", value = SizeOnlyStringifier.class)
        @IgniteToStringOrder(3)
        private final List<String> list1 = List.of("0", "1");

        @SuppressWarnings("UnusedPrivateField")
        private final Set<Long> missingSet = Set.of();

        @IgniteStringifier(FooBarStringifier.class)
        private int int0;

        @IgniteStringifier(name = "int1.changed", value = FooBarStringifier.class)
        private int int1;

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private static class FooBarStringifier implements Stringifier<Object> {
        @Override
        public String toString(Object instance) {
            return "foobar";
        }
    }

    @IgniteStringifier(FooBarStringifier.class)
    private static class ClassToStringOnly {
        private int missingInt;

        @IgniteStringifier(SizeOnlyStringifier.class)
        private final Set<Long> missingSet = Set.of();

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    @IgniteStringifier(name = "TestChangeName", value = FooBarStringifier.class)
    private static class ClassToStringOnlyWithChangeName extends ClassToStringOnly {
        // No-op.
    }
}
