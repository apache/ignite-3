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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests for parsing {@link IgniteProductVersion}.
 */
public class IgniteProductVersionTest {
    @Test
    void testValidVersions() {
        IgniteProductVersion version = IgniteProductVersion.fromString("3.2.4-SNAPSHOT");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 4));
        assertThat(version.patch(), is(nullValue()));
        assertThat(version.preRelease(), is(equalTo("SNAPSHOT")));
        assertThat(version.toString(), is(equalTo("3.2.4-SNAPSHOT")));

        version = IgniteProductVersion.fromString("3.2.4.5-SNAPSHOT");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 4));
        assertThat(version.patch(), is((byte) 5));
        assertThat(version.preRelease(), is(equalTo("SNAPSHOT")));
        assertThat(version.toString(), is(equalTo("3.2.4.5-SNAPSHOT")));

        version = IgniteProductVersion.fromString("1.2.3");

        assertThat(version.major(), is((byte) 1));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 3));
        assertThat(version.patch(), is(nullValue()));
        assertThat(version.preRelease(), is(nullValue()));
        assertThat(version.toString(), is(equalTo("1.2.3")));

        version = IgniteProductVersion.fromString("1.2.3.4");

        assertThat(version.major(), is((byte) 1));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 3));
        assertThat(version.patch(), is((byte) 4));
        assertThat(version.preRelease(), is(nullValue()));
        assertThat(version.toString(), is(equalTo("1.2.3.4")));

        version = IgniteProductVersion.fromString("3.1.2-alpha22");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 1));
        assertThat(version.maintenance(), is((byte) 2));
        assertThat(version.patch(), is(nullValue()));
        assertThat(version.preRelease(), is(equalTo("alpha22")));
        assertThat(version.toString(), is(equalTo("3.1.2-alpha22")));

        version = IgniteProductVersion.fromString("3.1.2.3-beta23");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 1));
        assertThat(version.maintenance(), is((byte) 2));
        assertThat(version.patch(), is((byte) 3));
        assertThat(version.preRelease(), is(equalTo("beta23")));
        assertThat(version.toString(), is(equalTo("3.1.2.3-beta23")));
    }

    @Test
    void testInvalidVersions() {
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("  "));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("1.2"));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("a.b.c"));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("a.b.c.d"));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("1.2.3-"));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("1.2.3-SNAPSHOT-alpha123"));
    }
}
