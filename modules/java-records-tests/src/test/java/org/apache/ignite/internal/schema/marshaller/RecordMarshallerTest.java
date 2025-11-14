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

package org.apache.ignite.internal.schema.marshaller;

import static org.apache.ignite.internal.schema.marshaller.AssertMarshaller.assertMarshaller;
import static org.apache.ignite.internal.schema.marshaller.AssertMarshaller.assertMarshallerThrows;

import org.apache.ignite.internal.schema.marshaller.Records.ComponentsExact;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsNarrow;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsReordered;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsWide;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsWrongTypes;
import org.apache.ignite.internal.schema.marshaller.Records.NotAnnotatedNotMapped;
import org.apache.ignite.lang.MarshallerException;
import org.junit.jupiter.api.Test;

/**
 * Ensures that records and classes behave the same way.
 */
class RecordMarshallerTest {

    @Test
    void marshalUnmarshalTest() {
        assertMarshaller(new ComponentsExact.Record(1, "a"));
        assertMarshaller(new ComponentsExact.Class(1, "a"));

        assertMarshaller(new ComponentsExact.ExplicitCanonical(1, "a"));
        assertMarshaller(new ComponentsExact.ExplicitCompact(1, "a"));
        assertMarshaller(new ComponentsExact.ExplicitMultiple(1, "a"));

        assertMarshaller(new ComponentsNarrow.Record(1));
        assertMarshaller(new ComponentsNarrow.Class(1));

        assertMarshaller(new ComponentsReordered.Record("a", 1));
        assertMarshaller(new ComponentsReordered.Class("a", 1));
    }

    @Test
    void componentsWideThrowsException() {
        String msgSubstring = "are not mapped to columns";
        assertMarshallerThrows(MarshallerException.class, msgSubstring, new ComponentsWide.Record(1, "a", "b", "c"));
        assertMarshallerThrows(MarshallerException.class, msgSubstring, new ComponentsWide.Class(1, "a", "b", "c"));
    }

    @Test
    void componentsWrongTypesThrowsException() {
        String msgSubstring = "Column's type mismatch";
        assertMarshallerThrows(ClassCastException.class, msgSubstring, new ComponentsWrongTypes.Record((short) 1, 2));
        assertMarshallerThrows(ClassCastException.class, msgSubstring, new ComponentsWrongTypes.Class((short) 1, 2));
    }

    @Test
    void notAnnotatedNotMappedThrowsException() {
        String msgSubstring = "No mapped object field found for column";
        assertMarshallerThrows(MarshallerException.class, msgSubstring, new NotAnnotatedNotMapped.Record(1, "a"));
        assertMarshallerThrows(MarshallerException.class, msgSubstring, new NotAnnotatedNotMapped.Class(1, "a"));
    }

    @Test
    void localClassNotSupported() {
        record R() {}

        class C {}

        String msgSubstring = "Unsupported class. Only top-level or nested static classes are supported";
        assertMarshallerThrows(IllegalArgumentException.class, msgSubstring, new R());
        assertMarshallerThrows(IllegalArgumentException.class, msgSubstring, new C());
    }
}
