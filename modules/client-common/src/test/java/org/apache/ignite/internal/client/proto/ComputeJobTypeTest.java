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

package org.apache.ignite.internal.client.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.apache.ignite.internal.client.proto.ComputeJobType.Type;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(Lifecycle.PER_CLASS)
class ComputeJobTypeTest {
    private static Stream<Arguments> marshalledTypes() {
        return Stream.of(
                Arguments.of(1, Type.MARSHALLED_TUPLE, 1),
                Arguments.of(2, Type.MARSHALLED_OBJECT, 2),
                Arguments.of(3, Type.NATIVE, 3)
        );
    }

    @MethodSource({"marshalledTypes"})
    @ParameterizedTest
    void supportedTypes(int givenId, ComputeJobType.Type expectedType, int expectedId) {
        ComputeJobType computeJobType = new ComputeJobType(givenId);

        assertEquals(expectedType, computeJobType.type());
        assertEquals(expectedId, computeJobType.id());
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypesArgs")
    void unsupportedTypes(int givenId) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new ComputeJobType(givenId));
        assertEquals("Unsupported type id: " + givenId, e.getMessage());
    }

    private Stream<Arguments> unsupportedTypesArgs() {
        return Stream.of(Integer.MIN_VALUE, Integer.MAX_VALUE).map(Arguments::of);
    }
}
