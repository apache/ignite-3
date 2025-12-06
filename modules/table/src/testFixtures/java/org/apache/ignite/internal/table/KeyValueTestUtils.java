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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;

import java.util.Objects;
import java.util.Random;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.table.QualifiedName;

/**
 * Key-value view test utilities.
 */
public class KeyValueTestUtils {
    public static final QualifiedName TEST_TABLE_NAME = QualifiedName.of("default", "table");

    public static final Column[] ALL_TYPES_COLUMNS = {
            new Column("primitiveBooleanCol".toUpperCase(), BOOLEAN, false),
            new Column("primitiveByteCol".toUpperCase(), INT8, false),
            new Column("primitiveShortCol".toUpperCase(), INT16, false),
            new Column("primitiveIntCol".toUpperCase(), INT32, false),
            new Column("primitiveLongCol".toUpperCase(), INT64, false),
            new Column("primitiveFloatCol".toUpperCase(), FLOAT, false),
            new Column("primitiveDoubleCol".toUpperCase(), DOUBLE, false),

            new Column("booleanCol".toUpperCase(), BOOLEAN, true),
            new Column("byteCol".toUpperCase(), INT8, true),
            new Column("shortCol".toUpperCase(), INT16, true),
            new Column("intCol".toUpperCase(), INT32, true),
            new Column("longCol".toUpperCase(), INT64, true),
            new Column("nullLongCol".toUpperCase(), INT64, true),
            new Column("floatCol".toUpperCase(), FLOAT, true),
            new Column("doubleCol".toUpperCase(), DOUBLE, true),

            new Column("dateCol".toUpperCase(), DATE, true),
            new Column("timeCol".toUpperCase(), time(0), true),
            new Column("dateTimeCol".toUpperCase(), datetime(6), true),
            new Column("timestampCol".toUpperCase(), timestamp(6), true),

            new Column("uuidCol".toUpperCase(), NativeTypes.UUID, true),
            new Column("stringCol".toUpperCase(), STRING, true),
            new Column("nullBytesCol".toUpperCase(), BYTES, true),
            new Column("bytesCol".toUpperCase(), BYTES, true),
            new Column("decimalCol".toUpperCase(), NativeTypes.decimalOf(19, 3), true),
    };

    static TestKeyObject newKey(Random rnd) {
        return TestKeyObject.randomObject(rnd);
    }

    static TestKeyObject newKey(long key) {
        return new TestKeyObject(key);
    }

    static TestObjectWithAllTypes newValue(Random rnd) {
        return TestObjectWithAllTypes.randomObject(rnd);
    }

    public static TupleMarshallerImpl createMarshaller(SchemaDescriptor schema) {
        return new TupleMarshallerImpl(() -> TEST_TABLE_NAME, schema);
    }

    /**
     * Test object.
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "unused"})
    public static class TestKeyObject {
        public static TestKeyObject randomObject(Random rnd) {
            return new TestKeyObject(rnd.nextLong());
        }

        private long id;

        private TestKeyObject() {
        }

        private TestKeyObject(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestKeyObject that = (TestKeyObject) o;

            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
