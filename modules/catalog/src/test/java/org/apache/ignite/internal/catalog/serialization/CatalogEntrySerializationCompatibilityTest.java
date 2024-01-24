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

package org.apache.ignite.internal.catalog.serialization;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify catalog storage entries serialization protocol compatibility.
 */
public class CatalogEntrySerializationCompatibilityTest {
    private final CatalogEntrySerializerProvider serializerProvider = (id) -> TestUpdateEntrySerializer.INSTANCE;

    @Test
    public void checkBackwardCompatibility() {
        {
            TestUpdateEntry entryV1 = TestEntryFactory.create(1);

            UpdateLogMarshaller marshallerV1 = new UpdateLogMarshallerImpl(1, serializerProvider);

            List<UpdateEntry> entries = List.of(entryV1, entryV1);

            VersionedUpdate update = new VersionedUpdate(1, 2, entries);

            byte[] bytesV1 = marshallerV1.marshall(update);

            VersionedUpdate deserialized = marshallerV1.unmarshall(bytesV1);
            assertThat(deserialized.entries(), equalTo(entries));

            UpdateLogMarshaller marshallerV2 = new UpdateLogMarshallerImpl(2, serializerProvider);
            deserialized = marshallerV2.unmarshall(bytesV1);
            assertThat(deserialized.entries(), equalTo(entries));

            UpdateLogMarshaller marshallerV3 = new UpdateLogMarshallerImpl(3, serializerProvider);
            deserialized = marshallerV3.unmarshall(bytesV1);
            assertThat(deserialized.entries(), equalTo(entries));
        }

        {
            TestUpdateEntryV2 entryV2 = TestEntryFactory.create(2);

            UpdateLogMarshaller marshallerV2 = new UpdateLogMarshallerImpl(2, serializerProvider);

            List<UpdateEntry> entries = List.of(entryV2, entryV2);
            VersionedUpdate update = new VersionedUpdate(1, 2, entries);

            byte[] bytesV2 = marshallerV2.marshall(update);

            VersionedUpdate deserialized = marshallerV2.unmarshall(bytesV2);
            assertThat(deserialized.entries(), equalTo(entries));

            UpdateLogMarshaller marshallerV3 = new UpdateLogMarshallerImpl(3, serializerProvider);
            deserialized = marshallerV3.unmarshall(bytesV2);
            assertThat(deserialized.entries(), equalTo(entries));
        }

        {
            TestUpdateEntryV3 entryV3 = TestEntryFactory.create(3);
            TestUpdateEntry entryV1 = TestEntryFactory.create(1);

            UpdateLogMarshaller marshallerV1 = new UpdateLogMarshallerImpl(1, serializerProvider);

            List<UpdateEntry> entries = List.of(entryV3, entryV3);
            VersionedUpdate update = new VersionedUpdate(1, 2, entries);

            byte[] bytesV1 = marshallerV1.marshall(update);

            VersionedUpdate deserialized = marshallerV1.unmarshall(bytesV1);
            assertThat(deserialized.entries(), equalTo(List.of(entryV1, entryV1)));
        }
    }

    @Test
    public void forwardCompatibilityIsNotSupported() {
        TestUpdateEntryV3 entryV3 = TestEntryFactory.create(3);

        List<UpdateEntry> entries = List.of(entryV3, entryV3);
        VersionedUpdate update = new VersionedUpdate(1, 2, entries);

        UpdateLogMarshaller marshallerV3 = new UpdateLogMarshallerImpl(3, serializerProvider);

        byte[] bytesV3 = marshallerV3.marshall(update);

        UpdateLogMarshaller marshallerV2 = new UpdateLogMarshallerImpl(2, serializerProvider);

        //noinspection ThrowableNotThrown
        assertThrowsWithCause(
                () -> marshallerV2.unmarshall(bytesV3),
                IllegalStateException.class,
                "An object could not be deserialized because it was using a newer version of the "
                        + "serialization protocol [objectVersion=3, supported=2]"
        );
    }

    static class TestEntryFactory {
        static <T extends TestUpdateEntry>  T create(int version) {
            TestDescriptor1 descriptor1 = new TestDescriptor1("name");

            TestUpdateEntry entry = new TestUpdateEntry(descriptor1, 42);

            if (version > 1) {
                entry = new TestUpdateEntryV2(entry.descriptor1, entry.value, List.of(new TestDescriptor2("strField", 43)));
            }

            if (version > 2) {
                entry = new TestUpdateEntryV3(entry.descriptor1, entry.value, ((TestUpdateEntryV2) entry).descList, "name2");
            }

            return (T) entry;
        }
    }

    static class TestDescriptor1 {
        final String name;

        TestDescriptor1(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestDescriptor1 that = (TestDescriptor1) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        static CatalogObjectSerializer<TestDescriptor1> serializer() {
            return new CatalogObjectSerializer<>() {
                @Override
                public TestDescriptor1 readFrom(int version, IgniteDataInput input) throws IOException {
                    return new TestDescriptor1(input.readUTF());
                }

                @Override
                public void writeTo(TestDescriptor1 descriptor, int version, IgniteDataOutput output) throws IOException {
                    output.writeUTF(descriptor.name);
                }
            };
        }
    }

    static class TestDescriptor2 {
        private final String strField;
        private final int intField;

        TestDescriptor2(String strField, int intField) {
            this.strField = strField;
            this.intField = intField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestDescriptor2 that = (TestDescriptor2) o;
            return intField == that.intField && Objects.equals(strField, that.strField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(strField, intField);
        }

        static CatalogObjectSerializer<TestDescriptor2> serializer() {
            return new CatalogObjectSerializer<>() {
                @Override
                public TestDescriptor2 readFrom(int version, IgniteDataInput input) throws IOException {
                    String strField = input.readUTF();
                    int intField = input.readInt();

                    return new TestDescriptor2(strField, intField);
                }

                @Override
                public void writeTo(TestDescriptor2 descriptor, int version, IgniteDataOutput output) throws IOException {
                    output.writeUTF(descriptor.strField);
                    output.writeInt(descriptor.intField);
                }
            };
        }
    }

    static class TestUpdateEntry implements UpdateEntry {
        final TestDescriptor1 descriptor1;
        final int value;

        TestUpdateEntry(TestDescriptor1 descriptor1, int value) {
            this.descriptor1 = descriptor1;
            this.value = value;
        }

        @Override
        public Catalog applyUpdate(Catalog catalog, long causalityToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int typeId() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestUpdateEntry that = (TestUpdateEntry) o;
            return value == that.value && Objects.equals(descriptor1, that.descriptor1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(descriptor1, value);
        }
    }

    static class TestUpdateEntryV2 extends TestUpdateEntry {
        final List<TestDescriptor2> descList;

        TestUpdateEntryV2(TestDescriptor1 descriptor1, int value, List<TestDescriptor2> descList) {
            super(descriptor1, value);

            this.descList = descList;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestUpdateEntryV2)) {
                return false;
            }

            TestUpdateEntryV2 that = (TestUpdateEntryV2) o;
            return Objects.equals(descList, that.descList) && value == that.value && Objects.equals(descriptor1, that.descriptor1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), descList);
        }
    }

    static class TestUpdateEntryV3 extends TestUpdateEntryV2 {
        final String name2;

        TestUpdateEntryV3(TestDescriptor1 descriptor1, int value, List<TestDescriptor2> descriptor2, String name2) {
            super(descriptor1, value, descriptor2);

            this.name2 = name2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestUpdateEntryV3 that = (TestUpdateEntryV3) o;
            return Objects.equals(name2, that.name2) && Objects.equals(descList, that.descList) && value == that.value
                    && Objects.equals(descriptor1, that.descriptor1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), name2);
        }
    }

    private static class TestUpdateEntrySerializer<T extends TestUpdateEntry> implements CatalogObjectSerializer<T> {
        @SuppressWarnings({"unchecked", "rawtypes"})
        private static final CatalogObjectSerializer<UpdateEntry> INSTANCE = new TestUpdateEntrySerializer();

        @Override
        public T readFrom(int version, IgniteDataInput input) throws IOException {
            TestDescriptor1 descriptor1 = TestDescriptor1.serializer().readFrom(version, input);
            int value = input.readInt();

            if (version < 2) {
                return (T) new TestUpdateEntry(descriptor1, value);
            }

            List<TestDescriptor2> descList = CatalogSerializationUtils.readList(version, TestDescriptor2.serializer(), input);

            if (version < 3) {
                return (T) new TestUpdateEntryV2(descriptor1, value, descList);
            }

            return (T) new TestUpdateEntryV3(descriptor1, value, descList, input.readUTF());
        }

        @Override
        public void writeTo(T entry, int version, IgniteDataOutput output) throws IOException {
            TestDescriptor1.serializer().writeTo(entry.descriptor1, version, output);
            output.writeInt(entry.value);

            if (version > 1) {
                assert entry instanceof TestUpdateEntryV2;

                CatalogSerializationUtils.writeList(((TestUpdateEntryV2) entry).descList, version, TestDescriptor2.serializer(), output);
            }

            if (version > 2) {
                assert entry instanceof TestUpdateEntryV3;

                output.writeUTF(((TestUpdateEntryV3) entry).name2);
            }
        }
    }
}
