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

package org.apache.ignite.deployment.version;

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
 * Unit tests for {@link Version}.
 */
public class VersionTest {
    private static Stream<Arguments> validVersions() {
        return Stream.of(
                arguments("1.1.0", version(1, 1, 0, null, null)),
                arguments("1.1.01", version(1, 1, 1, null, null)),
                arguments("1.1", version(1, 1, 0, null, null)),
                arguments("1", version(1, 0, 0, null, null)),
                arguments("1.1.1.1", version(1, 1, 1, 1, null)),
                arguments("1.2.3-patch1", version(1, 2, 3, null, "patch1")),
                arguments("latest", Version.LATEST)
        );
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("validVersions")
    public void testParseCorrect(String versionString, Version expected) {
        assertThat(Version.parseVersion(versionString), is(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "version", "1.", "65536", "1.1f"})
    public void testParseErrors(String versionString) {
        assertThrows(VersionParseException.class, () -> Version.parseVersion(versionString));
    }

    private static UnitVersion version(
            int major, int minor, int maintenance, @Nullable Integer patch, @Nullable String preRelease
    ) {
        Byte patchByte = patch != null ? patch.byteValue() : null;
        return new UnitVersion((byte) major, (byte) minor, (byte) maintenance, patchByte, preRelease);
    }
}
