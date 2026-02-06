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

package org.apache.ignite.table.mapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RecordSupportInternalTest {
    @Test
    void pojoIsNotRecordTest() throws IllegalAccessException {
        assertFalse(RecordSupportInternal.isRecord(TestPojo.class));
    }

    @Test
    void recordIsRecordTest() throws IllegalAccessException {
        assertTrue(RecordSupportInternal.isRecord(TestRecord.class));
    }

    @Test
    void pojoCanonicalConstructorTest() {
        assertThrows(IllegalArgumentException.class, () -> RecordSupportInternal.getCanonicalConstructor(TestPojo.class));
    }

    @Test
    void recordCanonicalConstructorTest() throws Exception {
        var constructor = RecordSupportInternal.getCanonicalConstructor(TestRecord.class);
        assertEquals(1, constructor.getParameterCount(), "Constructor must have one parameter");
        assertSame(int.class, constructor.getParameterTypes()[0], "Constructor must have one int parameter");

        assertDoesNotThrow(() -> constructor.newInstance(1));
    }

    record TestRecord(int id) {}

    static class TestPojo {
        final int id;

        TestPojo(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }
}
