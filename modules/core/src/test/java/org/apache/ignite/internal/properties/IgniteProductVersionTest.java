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

package org.apache.ignite.internal.properties;

import static org.apache.ignite.internal.properties.IgniteProductVersion.fromString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for parsing {@link IgniteProductVersion}.
 */
public class IgniteProductVersionTest {
    private static Stream<Arguments> validVersions() {
        return Stream.of(
                arguments("3.2.4-SNAPSHOT", version(3, 2, 4, null, "SNAPSHOT")),
                arguments("3.2.4.5-SNAPSHOT", version(3, 2, 4, 5, "SNAPSHOT")),
                arguments("1.2.3", version(1, 2, 3, null, null)),
                arguments("1.2.3.4", version(1, 2, 3, 4, null)),
                arguments("3.1.2-alpha22", version(3, 1, 2, null, "alpha22")),
                arguments("3.1.2.3-beta23", version(3, 1, 2, 3, "beta23"))
        );
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("validVersions")
    void testValidVersions(String versionString, IgniteProductVersion expected) {
        IgniteProductVersion version = fromString(versionString);

        assertThat(version.major(), is(expected.major()));
        assertThat(version.minor(), is(expected.minor()));
        assertThat(version.maintenance(), is(expected.maintenance()));
        assertThat(version.patch(), is(expected.patch()));
        assertThat(version.preRelease(), is(expected.preRelease()));
        assertThat(version.toString(), is(expected.toString()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"  ", "1.2", "a.b.c", "a.b.c.d", "1.2.3-", "1.2.3-SNAPSHOT-alpha123"})
    void testInvalidVersions(String versionsString) {
        assertThrows(IllegalArgumentException.class, () -> fromString(versionsString));
    }

    @ParameterizedTest(name = "[{index}] {0}.compareTo({1}) = {2}")
    @MethodSource("versionCompareProvider")
    void testCompareTo(IgniteProductVersion v1, IgniteProductVersion v2, int expected) {
        assertThat(Integer.signum(v1.compareTo(v2)), is(expected));
    }

    private static Stream<Arguments> versionCompareProvider() {
        return Stream.of(
                // major version differences
                arguments(fromString("2.0.0"), fromString("3.0.0"), -1),
                arguments(fromString("3.0.0"), fromString("2.0.0"), 1),
                arguments(fromString("3.0.0"), fromString("3.0.0"), 0),

                // minor version differences
                arguments(fromString("3.1.0"), fromString("3.2.0"), -1),
                arguments(fromString("3.2.0"), fromString("3.1.0"), 1),
                arguments(fromString("3.2.0"), fromString("3.2.0"), 0),

                // maintenance version differences
                arguments(fromString("3.2.1"), fromString("3.2.2"), -1),
                arguments(fromString("3.2.2"), fromString("3.2.1"), 1),
                arguments(fromString("3.2.2"), fromString("3.2.2"), 0),

                // patch version differences
                arguments(fromString("3.2.1.1"), fromString("3.2.1.2"), -1),
                arguments(fromString("3.2.1.2"), fromString("3.2.1.1"), 1),
                arguments(fromString("3.2.1.1"), fromString("3.2.1"), 1),
                arguments(fromString("3.2.1"), fromString("3.2.1.1"), -1),
                arguments(fromString("3.2.1.1"), fromString("3.2.1.1"), 0),

                // pre-release differences
                arguments(fromString("3.2.1-alpha"), fromString("3.2.1-beta"), -1),
                arguments(fromString("3.2.1-beta"), fromString("3.2.1-alpha"), 1),
                arguments(fromString("3.2.1-SNAPSHOT"), fromString("3.2.1-SNAPSHOT"), 0),
                arguments(fromString("3.2.1.1-SNAPSHOT"), fromString("3.2.1.1-SNAPSHOT"), 0),
                arguments(fromString("3.2.1-alpha"), fromString("3.2.1"), 1),
                arguments(fromString("3.2.1"), fromString("3.2.1-beta"), -1)
        );
    }

    private static IgniteProductVersion version(
            int major, int minor, int maintenance, @Nullable Integer patch, @Nullable String preRelease
    ) {
        Byte patchByte = patch != null ? patch.byteValue() : null;
        return new IgniteProductVersion((byte) major, (byte) minor, (byte) maintenance, patchByte, preRelease);
    }
}
