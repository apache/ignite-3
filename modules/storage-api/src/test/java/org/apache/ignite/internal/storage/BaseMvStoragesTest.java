/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage;

import java.util.Locale;
import java.util.Objects;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base test for MV storages, contains pojo classes, their descriptor and a marshaller instance.
 */
public abstract class BaseMvStoragesTest {
    /** Default reflection marshaller factory. */
    protected static MarshallerFactory marshallerFactory;

    /** Schema descriptor for tests. */
    protected static SchemaDescriptor schemaDescriptor;

    /** Key-value marshaller for tests. */
    protected static KvMarshaller<TestKey, TestValue> kvMarshaller;

    @BeforeAll
    static void beforeAll() {
        marshallerFactory = new ReflectionMarshallerFactory();

        schemaDescriptor = new SchemaDescriptor(1, new Column[]{
                new Column("intKey".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strKey".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        }, new Column[]{
                new Column("intVal".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strVal".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        });

        kvMarshaller = marshallerFactory.create(schemaDescriptor, TestKey.class, TestValue.class);
    }

    @AfterAll
    static void afterAll() {
        kvMarshaller = null;
        schemaDescriptor = null;
        marshallerFactory = null;
    }

    protected static BinaryRow binaryKey(TestKey key) {
        try {
            return kvMarshaller.marshal(key);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    protected static BinaryRow binaryRow(TestKey key, TestValue value) {
        try {
            return kvMarshaller.marshal(key, value);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    protected static TestKey key(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalKey(new Row(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    @Nullable
    protected static TestValue value(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalValue(new Row(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Test pojo key.
     */
    protected static class TestKey {
        public int intKey;

        public String strKey;

        public TestKey() {
        }

        public TestKey(int intKey, String strKey) {
            this.intKey = intKey;
            this.strKey = strKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestKey testKey = (TestKey) o;
            return intKey == testKey.intKey && Objects.equals(strKey, testKey.strKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intKey, strKey);
        }
    }

    /**
     * Test pojo value.
     */
    protected static class TestValue implements Comparable<TestValue> {
        @IgniteToStringInclude
        public Integer intVal;

        @IgniteToStringInclude
        public String strVal;

        public TestValue() {
        }

        public TestValue(Integer intVal, String strVal) {
            this.intVal = intVal;
            this.strVal = strVal;
        }

        @Override
        public int compareTo(TestValue o) {
            int cmp = Integer.compare(intVal, o.intVal);

            return cmp != 0 ? cmp : strVal.compareTo(o.strVal);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestValue testValue = (TestValue) o;
            return Objects.equals(intVal, testValue.intVal) && Objects.equals(strVal, testValue.strVal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intVal, strVal);
        }

        @Override
        public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
