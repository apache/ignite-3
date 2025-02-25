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

package org.apache.ignite.internal.catalog.storage.serialization.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.catalog.storage.serialization.VersionAwareSerializer;
import org.apache.ignite.internal.catalog.test.serializers.valid.TestNotSerializer;
import org.apache.ignite.internal.catalog.test.serializers.valid.TestSerializer1;
import org.apache.ignite.internal.catalog.test.serializers.valid.a.TestSerializer2;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for class {@link SerializerRegistryBuilder}.
 */
public class SerializerRegistryBuilderTest {
    private static final String BASE_PACKAGE = "org.apache.ignite.internal.catalog.test.serializers";

    @Test
    void testDuplicateVersion() {
        List<String> packageName = packageName("invalid.version.duplicate");
        SerializerRegistryBuilder builder = new SerializerRegistryBuilder(packageName, null);

        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrows(IllegalArgumentException.class, builder::build,
                "Duplicate serializer version");
    }

    @Test
    void testNegativeVersion() {
        List<String> packageName = packageName("invalid.version.negative");
        SerializerRegistryBuilder builder = new SerializerRegistryBuilder(packageName, null);

        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrows(IllegalArgumentException.class, builder::build,
                "Serializer version must be positive");
    }

    @Test
    void testMissingVersion() {
        List<String> packageName = packageName("invalid.version.skipped");
        SerializerRegistryBuilder builder = new SerializerRegistryBuilder(packageName, null);

        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                builder::build,
                "The serializer version must be incremented by one [type=1, ver=3, max=2"
        );
    }

    @Test
    void testSerializersOrderedByVersion() {
        List<String> packageName = packageName("valid");
        SerializerRegistryBuilder builder = new SerializerRegistryBuilder(packageName, null);

        Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> result = builder.build();

        VersionAwareSerializer<? extends MarshallableEntry>[] orderedSerializers = result.get(MarshallableEntryType.ALTER_COLUMN.id());

        assertThat(orderedSerializers.length, is(3));

        for (int i = 0; i < 3; i++) {
            assertThat("i=" + i, orderedSerializers[i].version(), is((short) (i + 1)));
        }
    }

    @Test
    void testScanClasspath() throws Exception {
        List<String> packageName = packageName("valid");

        {
            List<Class<?>> classes = SerializerRegistryBuilder.scanClasspaths(packageName,
                    cls -> cls.getSimpleName().startsWith("TestSerializer"));

            assertThat(classes, containsInAnyOrder(TestSerializer1.class, TestSerializer2.class));
        }

        {
            List<Class<?>> classes = SerializerRegistryBuilder.scanClasspaths(packageName,
                    cls -> cls.getSimpleName().contains("NotSerializer"));

            assertThat(classes, contains(TestNotSerializer.class));
        }
    }

    private static List<String> packageName(String suffix) {
        return List.of(BASE_PACKAGE + "." + suffix);
    }
}
