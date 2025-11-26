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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for parsing {@link IgniteProductVersion}.
 */
public class IgniteProductVersionTest {
    @Test
    void testValidVersions() {
        IgniteProductVersion version = fromString("3.2.4-SNAPSHOT");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 4));
        assertThat(version.patch(), is(nullValue()));
        assertThat(version.preRelease(), is(equalTo("SNAPSHOT")));
        assertThat(version.toString(), is(equalTo("3.2.4-SNAPSHOT")));

        version = fromString("3.2.4.5-SNAPSHOT");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 4));
        assertThat(version.patch(), is((byte) 5));
        assertThat(version.preRelease(), is(equalTo("SNAPSHOT")));
        assertThat(version.toString(), is(equalTo("3.2.4.5-SNAPSHOT")));

        version = fromString("1.2.3");

        assertThat(version.major(), is((byte) 1));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 3));
        assertThat(version.patch(), is(nullValue()));
        assertThat(version.preRelease(), is(nullValue()));
        assertThat(version.toString(), is(equalTo("1.2.3")));

        version = fromString("1.2.3.4");

        assertThat(version.major(), is((byte) 1));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 3));
        assertThat(version.patch(), is((byte) 4));
        assertThat(version.preRelease(), is(nullValue()));
        assertThat(version.toString(), is(equalTo("1.2.3.4")));

        version = fromString("3.1.2-alpha22");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 1));
        assertThat(version.maintenance(), is((byte) 2));
        assertThat(version.patch(), is(nullValue()));
        assertThat(version.preRelease(), is(equalTo("alpha22")));
        assertThat(version.toString(), is(equalTo("3.1.2-alpha22")));

        version = fromString("3.1.2.3-beta23");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 1));
        assertThat(version.maintenance(), is((byte) 2));
        assertThat(version.patch(), is((byte) 3));
        assertThat(version.preRelease(), is(equalTo("beta23")));
        assertThat(version.toString(), is(equalTo("3.1.2.3-beta23")));
    }

    @Test
    void testInvalidVersions() {
        assertThrows(IllegalArgumentException.class, () -> fromString("  "));
        assertThrows(IllegalArgumentException.class, () -> fromString("1.2"));
        assertThrows(IllegalArgumentException.class, () -> fromString("a.b.c"));
        assertThrows(IllegalArgumentException.class, () -> fromString("a.b.c.d"));
        assertThrows(IllegalArgumentException.class, () -> fromString("1.2.3-"));
        assertThrows(IllegalArgumentException.class, () -> fromString("1.2.3-SNAPSHOT-alpha123"));
    }

    @ParameterizedTest(name = "[{index}] {0}.compareTo({1}) = {2}")
    @MethodSource("versionCompareProvider")
    void testCompareTo(IgniteProductVersion v1, IgniteProductVersion v2, int expected) {
        assertThat(Integer.signum(v1.compareTo(v2)), is(expected));
    }

    static Stream<Arguments> versionCompareProvider() {
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
}
