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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Named.named;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for class {@link CatalogEntrySerializerProviderImpl}.
 */
@SuppressWarnings({"ThrowableNotThrown", "ResultOfObjectAllocationIgnored"})
public class CatalogEntrySerializerProviderImplTest {
    @Test
    void testGetByVersion() {
        List<CatalogSerializerTypeDefinition> serializerTypes = List.of(
                newType(0, Container1.class),
                newType(1, Container2.class)
        );

        CatalogEntrySerializerProvider registry = new CatalogEntrySerializerProviderImpl(serializerTypes);

        for (int i = 1; i < 4; i++) {
            CatalogVersionAwareSerializer<MarshallableEntry> serializer =
                    (CatalogVersionAwareSerializer<MarshallableEntry>) registry.get(i, 0);
            assertThat(serializer.version(), is((short) i));
        }

        for (int i = 1; i < 3; i++) {
            CatalogVersionAwareSerializer<MarshallableEntry> serializer =
                    (CatalogVersionAwareSerializer<MarshallableEntry>) registry.get(i, 1);
            assertThat(serializer.version(), is((short) i));
        }

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> registry.get(0, 0),
                "Serializer version must be positive [version=0]"
        );

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> registry.get(-1, 0),
                "Serializer version must be positive [version=-1]"
        );

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> registry.get(4, 0),
                "Required serializer version not found [version=4]"
        );
    }

    @Test
    void testLatestVersion() {
        List<CatalogSerializerTypeDefinition> serializerTypes = List.of(
                newType(0, Container1.class),
                newType(1, Container2.class)
        );

        CatalogEntrySerializerProvider registry = new CatalogEntrySerializerProviderImpl(serializerTypes);

        assertThat(registry.latestSerializerVersion(0), is(3));
        assertThat(registry.latestSerializerVersion(1), is(2));
    }

    @Test
    void testSerializerWithPrivateConstructor() {
        List<CatalogSerializerTypeDefinition> types = List.of(
                newType(0, ContainerWithSerializerWithPrivateConstructor.class)
        );

        CatalogEntrySerializerProvider provider = new CatalogEntrySerializerProviderImpl(types);

        assertThat(provider.get(1, 0), notNullValue());
        assertThat(provider.get(2, 0), notNullValue());
    }

    @ParameterizedTest
    @MethodSource("typesWithEmptyContainers")
    void testSerializerNotFoundInContainer(List<CatalogSerializerTypeDefinition> types) {
        IgniteTestUtils.assertThrows(
                IllegalStateException.class,
                () -> new CatalogEntrySerializerProviderImpl(types),
                "At least one serializer must be implemented [type=typeName]."
        );
    }

    @Test
    void testSerializerWithInvalidVersion() {
        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> new CatalogEntrySerializerProviderImpl(List.of(newType(0, ContainerWithInvalidVersion1.class))),
                "Serializer `version` attribute must be positive [version=0,"
        );

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> new CatalogEntrySerializerProviderImpl(List.of(newType(0, ContainerWithInvalidVersion2.class))),
                "Serializer `version` attribute must be positive [version=-1,"
        );
    }

    @Test
    void testContainerWithMissingVersion() {
        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> new CatalogEntrySerializerProviderImpl(List.of(newType(3, ContainerWithMissingVersion.class))),
                "Serializer version must be incremented by one [typeId=3, version=3, expected=2"
        );
    }

    @Test
    void testContainerWithDuplicateVersion() {
        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> new CatalogEntrySerializerProviderImpl(List.of(newType(3, ContainerWithDuplicateVersion.class))),
                "Duplicate serializer version"
        );
    }

    @Test
    void testNullOrBlankSinceIsForbidden() {
        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> new CatalogEntrySerializerProviderImpl(List.of(newType(0, ContainerWithInvalidSince1.class))),
                "Serializer 'since' attribute can't be empty or blank"
        );

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> new CatalogEntrySerializerProviderImpl(List.of(newType(0, ContainerWithInvalidSince2.class))),
                "Serializer 'since' attribute can't be empty or blank"
        );
    }

    @Test
    void testSerializerWithInvalidConstructor() {
        IgniteTestUtils.assertThrows(
                IllegalStateException.class,
                () -> new CatalogEntrySerializerProviderImpl(List.of(newType(0, ContainerWithSerializerWithInvalidConstructor.class))),
                "Unable to create serializer, required constructor was not found"
        );
    }

    @Test
    void testAnnotatedNotSerializer() {
        List<CatalogSerializerTypeDefinition> types = List.of(
                newType(0, ContainerWithAnnotatedNotSerializer.class)
        );

        IgniteTestUtils.assertThrows(
                IllegalStateException.class,
                () -> new CatalogEntrySerializerProviderImpl(types),
                "The target class doesn't implement the required interface"
        );
    }

    private static Stream<Arguments> typesWithEmptyContainers() {
        return Stream.of(
                Arguments.of(named("no serializer", List.of(newType(0, ContainerWithoutSerializers.class)))),
                Arguments.of(named("not annotated serializer", List.of(newType(1, ContainerNotAnnotatedSerializer.class))))
        );
    }

    private static CatalogSerializerTypeDefinition newType(int id, Class<?> container) {
        return new CatalogSerializerTypeDefinition() {
            @Override
            public int id() {
                return id;
            }

            @Override
            public Class<?> container() {
                return container;
            }

            @Override
            public String toString() {
                return "typeName";
            }
        };
    }

    static class Container1 {
        @CatalogSerializer(version = 3, since = "3.0.0")
        static class Serializer2 extends DummySerializer {}

        @CatalogSerializer(version = 1, since = "3.0.0")
        static class Serializer1 extends DummySerializer {}

        @CatalogSerializer(version = 2, since = "3.0.0")
        static class Serializer3 extends DummySerializer {}
    }

    static class ContainerWithSerializerWithPrivateConstructor {
        @CatalogSerializer(version = 1, since = "3.0.0")
        static class Serializer1 extends DummySerializer {
            private Serializer1() {

            }
        }

        @CatalogSerializer(version = 2, since = "3.0.0")
        static class Serializer2 extends DummySerializer {
            Serializer2() {

            }
        }
    }

    static class ContainerWithSerializerWithInvalidConstructor {
        @CatalogSerializer(version = 1, since = "3.0.0")
        static class Serializer1 extends DummySerializer {
            @SuppressWarnings("PMD.UnusedFormalParameter")
            Serializer1(int a) {

            }
        }
    }

    static class Container2 {
        @CatalogSerializer(version = 1, since = "3.0.0")
        static class Serializer1 extends DummySerializer {}

        @CatalogSerializer(version = 2, since = "3.0.0")
        static class Serializer2 extends DummySerializer {}
    }

    static class ContainerWithInvalidVersion1 {
        @CatalogSerializer(version = 0, since = "a")
        static class InvalidVersionSerializer extends DummySerializer {}
    }

    static class ContainerWithInvalidVersion2 {
        @CatalogSerializer(version = -1, since = "a")
        static class InvalidVersionSerializer extends DummySerializer {}
    }

    static class ContainerWithMissingVersion {
        @CatalogSerializer(version = 1, since = "a")
        static class Serializer1 extends DummySerializer {}

        @CatalogSerializer(version = 3, since = "a")
        static class Serializer2 extends DummySerializer {}
    }

    static class ContainerWithDuplicateVersion {
        @CatalogSerializer(version = 1, since = "a")
        static class Serializer1 extends DummySerializer {}

        @CatalogSerializer(version = 1, since = "a")
        static class Serializer2 extends DummySerializer {}
    }

    static class ContainerWithInvalidSince1 {
        @CatalogSerializer(version = 1, since = "")
        static class InvalidVersionSerializer extends DummySerializer {}
    }

    static class ContainerWithInvalidSince2 {
        @CatalogSerializer(version = 1, since = " ")
        static class InvalidVersionSerializer extends DummySerializer {}
    }

    static class ContainerWithoutSerializers {
        static class Foo {}

        static class Bar {}
    }

    static class ContainerNotAnnotatedSerializer {
        static class Serializer1 extends DummySerializer {
        }
    }

    static class ContainerWithAnnotatedNotSerializer {
        @CatalogSerializer(version = 1, since = "0")
        static class A {

        }
    }

    static class DummySerializer implements CatalogObjectSerializer<MarshallableEntry> {
        @Override
        public MarshallableEntry readFrom(CatalogObjectDataInput input) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(MarshallableEntry value, CatalogObjectDataOutput output) {
            throw new UnsupportedOperationException();
        }
    }
}
