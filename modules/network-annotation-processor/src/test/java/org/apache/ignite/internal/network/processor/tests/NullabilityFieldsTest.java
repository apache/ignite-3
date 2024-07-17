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

package org.apache.ignite.internal.network.processor.tests;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Tests for messages with and without {@link org.jetbrains.annotations.Nullable} fields. */
public class NullabilityFieldsTest {
    private final TestMessagesFactory factory = new TestMessagesFactory();

    @Test
    public void testNotNullArbitraryFieldBuildWithNull() {
        assertThrows(NullPointerException.class, () -> {
            // Should throw NPE from setter.
            factory.notNullArbitraryFieldMessage().value(null);
        });
        assertThrows(NullPointerException.class, () -> {
            // Should throw NPE from build method.
            factory.notNullArbitraryFieldMessage().build();
        });
    }

    @Test
    public void testNotNullArbitraryFieldBuild() {
        assertDoesNotThrow(() -> {
            // Build with value.
            factory.notNullArbitraryFieldMessage().value(UUID.randomUUID()).build();
        });
    }

    @Test
    public void testNotNullMarshallableFieldBuildWithNull() {
        assertThrows(NullPointerException.class, () -> {
            // Should throw NPE from constructor.
            factory.notNullMarshallableFieldMessage().build();
        });
    }

    @Test
    public void testNotNullMarshallableFieldBuild() {
        assertDoesNotThrow(() -> {
            // Build with value.
            factory.notNullMarshallableFieldMessage().value(new Object()).build();

            // Build with byte array representation of a value.
            factory.notNullMarshallableFieldMessage().valueByteArray(new byte[0]).build();
        });
    }

    @Test
    public void testNotNullNetworkMessageFieldBuild() {
        assertDoesNotThrow(() -> {
            // Build with value.
            factory.notNullNetworkMessageFieldMessage().value(
                    factory.notNullArbitraryFieldMessage().value(UUID.randomUUID()).build()
            ).build();
        });
    }

    @Test
    public void testNotNullArrayFieldBuildWithNull() {
        assertThrows(NullPointerException.class, () -> {
            factory.notNullArrayFieldMessage().value(null).build();
        });
        assertThrows(NullPointerException.class, () -> {
            factory.notNullArrayFieldMessage().build();
        });
    }

    @Test
    public void testNotNullArrayFieldBuild() {
        assertDoesNotThrow(() -> {
            // Build with value.
            factory.notNullArrayFieldMessage().value(new int[0]).build();
        });
    }

    @Test
    public void testNullableArbitraryFieldWithNull() {
        assertDoesNotThrow(() -> {
            factory.nullableArbitraryFieldMessage().value(null).build();
            factory.nullableArbitraryFieldMessage().build();
        });
    }

    @Test
    public void testNullableMarshallableFieldWithNull() {
        assertDoesNotThrow(() -> {
            factory.nullableMarshallableFieldMessage().value(null).build();
            factory.nullableMarshallableFieldMessage().build();
        });
    }

    @Test
    public void testNullableNetworkMessageFieldWithNull() {
        assertDoesNotThrow(() -> {
            factory.nullableNetworkMessageFieldMessage().value(null).build();
            factory.nullableNetworkMessageFieldMessage().build();
        });
    }

    @Test
    public void testNullableArrayFieldWithNull() {
        assertDoesNotThrow(() -> {
            factory.nullableArrayFieldMessage().value(null).build();
            factory.nullableArrayFieldMessage().build();
        });
    }
}
