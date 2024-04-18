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

package org.apache.ignite.internal.network.processor;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaFileObjects.forSourceLines;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;
import javax.tools.JavaFileObject;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.network.processor.messages.MarshallableTypesBlackList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test case for the marshallable types blacklist.
 */
public class MarshallableBlacklistTest {
    private static final JavaFileObject MESSAGE_GROUP = forResource("org/apache/ignite/internal/network/processor/TestMessageGroup.java");

    /**
     * Compiler instance configured with the annotation processor being tested.
     */
    private final Compiler compiler = Compiler.javac().withProcessors(new TransferableObjectProcessor());

    private static List<String> marshallableFieldSourceCode(Class<?> fieldType) {
        return testMessageSourceCode(String.format("%s foo();", fieldType.getName()));
    }

    private static List<String> marshallableCollectionSourceCode(Class<?> collectionType, Class<?> fieldType) {
        return testMessageSourceCode(String.format("%s<%s> foo();", collectionType.getName(), fieldType.getName()));
    }

    private static List<String> marshallableArraySourceCode(Class<?> fieldType) {
        return testMessageSourceCode(String.format("%s[] foo();", fieldType.getName()));
    }

    private static List<String> marshallableMapSourceCode(Class<?> keyType, Class<?> valueType) {
        return testMessageSourceCode(String.format("%s<%s, %s> foo();", Map.class.getName(), keyType.getName(), valueType.getName()));
    }

    private static List<String> marshallableNestedMapSourceCode(Class<?> keyType, Class<?> valueType) {
        return testMessageSourceCode(String.format(
                "%s<%s, %s<%s>> foo();",
                Map.class.getName(), keyType.getName(), List.class.getName(), valueType.getName())
        );
    }

    private static List<String> testMessageSourceCode(String fieldDeclaration) {
        return List.of(
                "package org.apache.ignite.internal.network.processor;",

                "import org.apache.ignite.internal.network.message.ScaleCubeMessage;",
                "import org.apache.ignite.internal.network.NetworkMessage;",
                "import org.apache.ignite.internal.network.annotations.Transferable;",
                "import org.apache.ignite.internal.network.annotations.Marshallable;",

                "@Transferable(1)",
                "public interface TestMessage extends NetworkMessage {",
                "    @Marshallable",
                fieldDeclaration,
                "}"
        );
    }

    private static List<Class<?>> nativeTypes() {
        return MarshallableTypesBlackList.NATIVE_TYPES;
    }

    private static Stream<Arguments> collectionTypes() {
        return Stream.of(Collection.class, List.class, Set.class)
                .flatMap(collectionType -> nativeTypes().stream().map(type -> arguments(collectionType, type)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeTypes")
    void testMessageWithNativeTypeField(Class<?> type) {
        Compilation compilation = compile(marshallableFieldSourceCode(type));

        assertThat(compilation).hadErrorContaining(
                "\"foo\" field is marked as @Marshallable but this type is either directly supported by native serialization"
        );
    }

    @ParameterizedTest(name = "{0}[]")
    @MethodSource("nativeTypes")
    void testMessageWithNativeArrayTypeField(Class<?> type) {
        Compilation compilation = compile(marshallableArraySourceCode(type));

        assertThat(compilation).hadErrorContaining(
                "\"foo\" field is marked as @Marshallable but this type is either directly supported by native serialization"
        );
    }

    @ParameterizedTest(name = "{0}<{1}>")
    @MethodSource("collectionTypes")
    void testMessageWithCollectionNativeTypeField(Class<?> collectionType, Class<?> type) {
        Compilation compilation = compile(marshallableCollectionSourceCode(collectionType, type));

        assertThat(compilation).hadErrorContaining(
                "\"foo\" field is marked as @Marshallable but this type is either directly supported by native serialization"
        );
    }

    @Test
    void testMessageWithMarshallableMapNativeKey() {
        Compilation compilation = compile(marshallableMapSourceCode(Integer.class, Lock.class));

        assertThat(compilation).succeededWithoutWarnings();
    }

    @Test
    void testMessageWithMarshallableMapNativeValue() {
        Compilation compilation = compile(marshallableMapSourceCode(Lock.class, Integer.class));

        assertThat(compilation).succeededWithoutWarnings();
    }

    @Test
    void testMessageWithNativeTypeMap() {
        Compilation compilation = compile(marshallableMapSourceCode(Integer.class, String.class));

        assertThat(compilation).hadErrorContaining(
                "\"foo\" field is marked as @Marshallable but this type is either directly supported by native serialization"
        );
    }

    @Test
    void testMessageWithNestedNativeTypeMap() {
        Compilation compilation = compile(marshallableNestedMapSourceCode(Integer.class, String.class));

        assertThat(compilation).hadErrorContaining(
                "\"foo\" field is marked as @Marshallable but this type is either directly supported by native serialization"
        );
    }

    @Test
    void testMessageWithNestedMarshallableTypeMap() {
        Compilation compilation = compile(marshallableNestedMapSourceCode(Integer.class, Lock.class));

        assertThat(compilation).succeededWithoutWarnings();
    }

    /**
     * Tests that compilation fails if message's field is both {@link NetworkMessage} and marked
     * as {@link Marshallable}.
     */
    @Test
    void testMessageWithMarshallableMessage() {
        Compilation compilation = compile(marshallableFieldSourceCode(ScaleCubeMessage.class));

        assertThat(compilation).hadErrorContaining(
                "\"foo\" field is marked as @Marshallable but this type is either directly supported by native serialization"
        );
    }

    @Test
    void testMessageWithBlockedByFileType() {
        Compilation compilation = compile(marshallableFieldSourceCode(Random.class));

        assertThat(compilation).hadErrorContaining(
                "\"foo\" field is marked as @Marshallable but this type is either directly supported by native serialization"
        );
    }

    private Compilation compile(List<String> code) {
        return compiler.compile(
                forSourceLines("org.apache.ignite.internal.network.processor.TestMessage", code),
                MESSAGE_GROUP
        );
    }
}
