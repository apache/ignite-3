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
import java.util.ArrayList;
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
    public void testReadWriteEntryList() throws IOException {
        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntryVar1 entry1 = new TestEntryVar1(random.nextInt());
            TestEntryVar2 entry2 = new TestEntryVar2(random.nextInt());

            output.writeEntry(entry1);
            output.writeEntry(entry2);

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                TestEntryVar1 actual1 = input.readEntry(TestEntryVar1.class);
                TestEntryVar2 actual2 = input.readEntry(TestEntryVar2.class);

                expectEntries(List.of(entry1, entry2), List.of(actual1, actual2));
            }
        }
    }

    @Test
    public void testReadWriteCompactEntryList() throws IOException {
        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            TestEntryVar1 entry1 = new TestEntryVar1(random.nextInt());
            TestEntryVar1 entry2 = new TestEntryVar1(random.nextInt());

            output.writeCompactEntryList(List.of(entry1, entry2));

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                List<BaseTestEntry> list = input.readCompactEntryList(BaseTestEntry.class);

                expectEntries(List.of(entry1, entry2), list);
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
    public void testReadWriteLatestEntry() throws Exception {
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

    @Test
    public void testReadWriteObjectList() throws IOException {
        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(provider)) {
            List<Map.Entry<Integer, String>> list = List.of(Map.entry(1, "42"), Map.entry(2, "109"));

            output.writeObjectCollection((out, element) -> {
                out.writeInt(element.getKey());
                out.writeUTF(element.getValue());
            }, list);

            byte[] data = output.array();
            try (CatalogObjectDataInput input = new CatalogObjectDataInput(provider, data)) {
                List<Map.Entry<Integer, String>> actual = input.readObjectCollection(
                        in -> Map.entry(in.readInt(), in.readUTF()), ArrayList::new);
                assertEquals(list, actual);
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

        TestCatalogEntrySerializerProvider() {
            serializerMap.put(Map.entry(TestEntryVar1.TYPE_ID, 2), new SimpleTestEntrySerializer(TestEntryVar1::new));
            serializerMap.put(Map.entry(TestEntryVar2.TYPE_ID, 3), new SimpleTestEntrySerializer(TestEntryVar2::new));
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
