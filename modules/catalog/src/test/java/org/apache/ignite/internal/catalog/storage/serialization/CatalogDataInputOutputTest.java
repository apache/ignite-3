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

package org.apache.ignite.internal.catalog.storage.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.function.IntFunction;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CatalogObjectDataOutput} / {@link CatalogObjectDataInput}.
 */
public class CatalogDataInputOutputTest extends BaseIgniteAbstractTest {

    private final TestCatalogEntrySerializerProvider provider = new TestCatalogEntrySerializerProvider();

    private final Random random = new Random();

    @Test
    public void testReadWriteEntry() throws IOException {
        MarshallableType<TestEntry> type = MarshallableType.typeOf(TestEntry.class, TestEntry.TYPE_ID, 2);

        provider.addSerializer(TestEntry.TYPE_ID, 2, new SimpleTestEntrySerializer(TestEntry::new));

        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntry entry1 = new TestEntry(random.nextInt());
            TestEntry entry2 = new TestEntry(random.nextInt());

            output.writeEntry(type, entry1);
            output.writeEntry(type, entry2);

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                TestEntry actual1 = input.readEntry(type);
                TestEntry actual2 = input.readEntry(type);

                expectEntries(List.of(entry1, entry2), List.of(actual1, actual2));
            }
        }
    }

    @Test
    public void testReadWriteEntryVariants() throws IOException {
        MarshallableType<BaseTestEntry> type = MarshallableType.builder(BaseTestEntry.class)
                .addVariant(TestEntry.TYPE_ID, 11)
                .addVariant(TestEntryVar1.TYPE_ID, 12)
                .addVariant(TestEntryVar2.TYPE_ID, 13)
                .build();

        provider.addSerializer(TestEntry.TYPE_ID, 11, new SimpleTestEntrySerializer(TestEntry::new));
        provider.addSerializer(TestEntryVar1.TYPE_ID, 12, new SimpleTestEntrySerializer(TestEntryVar1::new));
        provider.addSerializer(TestEntryVar2.TYPE_ID, 13, new SimpleTestEntrySerializer(TestEntryVar2::new));

        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntry entry1 = new TestEntry(random.nextInt());
            TestEntryVar1 entry2 = new TestEntryVar1(random.nextInt());
            TestEntryVar2 entry3 = new TestEntryVar2(random.nextInt());

            output.writeEntry(type, entry1);
            output.writeEntry(type, entry2);
            output.writeEntry(type, entry3);

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                BaseTestEntry actual1 = input.readEntry(type);
                BaseTestEntry actual2 = input.readEntry(type);
                BaseTestEntry actual3 = input.readEntry(type);

                expectEntries(List.of(entry1, entry2, entry3), List.of(actual1, actual2, actual3));
            }
        }
    }

    @Test
    public void testReadWriteEntryList() throws IOException {
        MarshallableType<TestEntry> type = MarshallableType.typeOf(TestEntry.class, TestEntry.TYPE_ID, 2);

        provider.addSerializer(TestEntry.TYPE_ID, 2, new SimpleTestEntrySerializer(TestEntry::new));

        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntry entry1 = new TestEntry(random.nextInt());
            TestEntry entry2 = new TestEntry(random.nextInt());

            output.writeEntryList(type, List.of(entry1, entry2));

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                List<TestEntry> list = input.readEntryList(type);

                expectEntries(List.of(entry1, entry2), list);
            }
        }
    }

    @Test
    public void testReadWriteEntryListVariants() throws IOException {
        MarshallableType<BaseTestEntry> type = MarshallableType.builder(BaseTestEntry.class)
                .addVariant(TestEntry.TYPE_ID, 11)
                .addVariant(TestEntryVar1.TYPE_ID, 12)
                .addVariant(TestEntryVar2.TYPE_ID, 13)
                .build();

        provider.addSerializer(TestEntry.TYPE_ID, 11, new SimpleTestEntrySerializer(TestEntry::new));
        provider.addSerializer(TestEntryVar1.TYPE_ID, 12, new SimpleTestEntrySerializer(TestEntryVar1::new));
        provider.addSerializer(TestEntryVar2.TYPE_ID, 13, new SimpleTestEntrySerializer(TestEntryVar2::new));

        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntry entry1 = new TestEntry(random.nextInt());
            TestEntryVar1 entry2 = new TestEntryVar1(random.nextInt());
            TestEntryVar2 entry3 = new TestEntryVar2(random.nextInt());

            output.writeEntryList(type, List.of(entry1, entry2, entry3));

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                List<BaseTestEntry> list = input.readEntryList(type);

                expectEntries(List.of(entry1, entry2, entry3), list);
            }
        }
    }

    @Test
    public void testReadWriteEntryArray() throws IOException {
        MarshallableType<TestEntry> type = MarshallableType.typeOf(TestEntry.class, TestEntry.TYPE_ID, 2);

        provider.addSerializer(TestEntry.TYPE_ID, 2, new SimpleTestEntrySerializer(TestEntry::new));

        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntry entry1 = new TestEntry(random.nextInt());
            TestEntry entry2 = new TestEntry(random.nextInt());

            output.writeEntryArray(type, new TestEntry[]{entry1, entry2});

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                BaseTestEntry[] list = input.readEntryArray(type);
                expectEntries(List.of(entry1, entry2), List.of(list));
            }
        }
    }

    @Test
    public void testReadWriteEntryArrayVariants() throws IOException {
        MarshallableType<BaseTestEntry> type = MarshallableType.builder(BaseTestEntry.class)
                .addVariant(TestEntry.TYPE_ID, 11)
                .addVariant(TestEntryVar1.TYPE_ID, 12)
                .addVariant(TestEntryVar2.TYPE_ID, 13)
                .build();

        provider.addSerializer(TestEntry.TYPE_ID, 11, new SimpleTestEntrySerializer(TestEntry::new));
        provider.addSerializer(TestEntryVar1.TYPE_ID, 12, new SimpleTestEntrySerializer(TestEntryVar1::new));
        provider.addSerializer(TestEntryVar2.TYPE_ID, 13, new SimpleTestEntrySerializer(TestEntryVar2::new));

        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntry entry1 = new TestEntry(random.nextInt());
            TestEntryVar1 entry2 = new TestEntryVar1(random.nextInt());
            TestEntryVar2 entry3 = new TestEntryVar2(random.nextInt());

            output.writeEntryArray(type, new BaseTestEntry[]{entry1, entry2, entry3});

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                BaseTestEntry[] list = input.readEntryArray(type);

                expectEntries(List.of(entry1, entry2, entry3), List.of(list));
            }
        }
    }

    private static void expectEntries(List<? extends BaseTestEntry> expected, List<? extends BaseTestEntry> actual) {
        assertEquals(expected.size(), actual.size());


        for (int i = 0; i < expected.size(); i++) {
            BaseTestEntry e = expected.get(i);
            BaseTestEntry a = actual.get(i);
            assertEquals(e.value, a.value, "value entry #" + i);
            assertEquals(e.typeId(), a.typeId(), "typeId entry #" + i);
        }
    }

    @Test
    public void testReadWriteLatest() throws Exception {
        provider.addSerializer(TestEntryVar1.TYPE_ID, 2, new SimpleTestEntrySerializer(TestEntryVar1::new));
        provider.addSerializer(TestEntryVar2.TYPE_ID, 3, new SimpleTestEntrySerializer(TestEntryVar2::new));

        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntryVar1 entry1 = new TestEntryVar1(random.nextInt());
            TestEntryVar2 entry2 = new TestEntryVar2(random.nextInt());

            output.writeEntry(entry1);
            output.writeEntry(entry2);

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                TestEntryVar1 actual1 = (TestEntryVar1) input.readEntry();
                TestEntryVar2 actual2 = (TestEntryVar2) input.readEntry();

                expectEntries(List.of(entry1, entry2), List.of(actual1, actual2));
            }
        }
    }

    private static class SimpleTestEntrySerializer implements CatalogObjectSerializer<BaseTestEntry> {

        private final IntFunction<BaseTestEntry> newEntry;

        private SimpleTestEntrySerializer(IntFunction<BaseTestEntry> newEntry) {
            this.newEntry = newEntry;
        }

        @Override
        public BaseTestEntry readFrom(CatalogObjectDataInput input) throws IOException {
            return newEntry.apply(input.readVarIntAsInt());
        }

        @Override
        public void writeTo(BaseTestEntry value, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(value.value);
        }
    }

    private static class TestCatalogEntrySerializerProvider implements CatalogEntrySerializerProvider {

        private final Map<Map.Entry<Integer, Integer>, CatalogObjectSerializer<?>> serializerMap = new HashMap<>();

        public void addSerializer(int typeId, int version, CatalogObjectSerializer<?> serializer) {
            serializerMap.put(Map.entry(typeId, version), serializer);
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <T extends MarshallableEntry> CatalogObjectSerializer<T> get(int version, int typeId) {
            Entry<Integer, Integer> versionKey = Map.entry(typeId, version);
            CatalogObjectSerializer serializer = serializerMap.get(versionKey);

            Objects.requireNonNull(serializer, "No serializer for " + versionKey + " available: " + serializerMap.keySet());
            return serializer;
        }

        @Override
        public int latestSerializerVersion(int typeId) {
            for (var versionKey : serializerMap.keySet()) {
                if (Objects.equals(typeId, versionKey.getKey())) {
                    return versionKey.getValue();
                }
            }
            throw new IllegalStateException("Unexpected type: " + typeId);
        }
    }

    private abstract static class BaseTestEntry implements MarshallableEntry {

        final int value;

        private BaseTestEntry(int value) {
            this.value = value;
        }
    }

    private static class TestEntry extends BaseTestEntry {

        private static final int TYPE_ID = 40;

        private TestEntry(int val) {
            super(val);
        }

        @Override
        public int typeId() {
            return TYPE_ID;
        }
    }

    private static class TestEntryVar1 extends BaseTestEntry {

        private static final int TYPE_ID = 41;

        private TestEntryVar1(int val) {
            super(val);
        }

        @Override
        public int typeId() {
            return TYPE_ID;
        }
    }

    private static class TestEntryVar2 extends BaseTestEntry {

        private static final int TYPE_ID = 42;

        private TestEntryVar2(int val) {
            super(val);
        }

        @Override
        public int typeId() {
            return TYPE_ID;
        }
    }
}
