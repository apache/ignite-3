/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests for parsing {@link IgniteProductVersion}.
 */
public class IgniteProductVersionTest {
    @Test
    void testValidVersions() {
        IgniteProductVersion version = IgniteProductVersion.fromString("3.0.0-SNAPSHOT");

        assertThat(version.major(), is((byte) 3));
        assertThat(version.minor(), is((byte) 0));
        assertThat(version.maintenance(), is((byte) 0));
        assertThat(version.snapshot(), is(true));
        assertThat(version.toString(), is(equalTo("3.0.0-SNAPSHOT")));

        version = IgniteProductVersion.fromString("1.2.3");

        assertThat(version.major(), is((byte) 1));
        assertThat(version.minor(), is((byte) 2));
        assertThat(version.maintenance(), is((byte) 3));
        assertThat(version.snapshot(), is(false));
        assertThat(version.toString(), is(equalTo("1.2.3")));
    }

    @Test
    void testInvalidVersions() {
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("  "));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("1.2"));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("a.b.c"));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("1.2.3-"));
        assertThrows(IllegalArgumentException.class, () -> IgniteProductVersion.fromString("1.2.3-SSDAD"));
    }
}
