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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Version}.
 */
public class VersionTest {
    @Test
    public void testParseCorrect() {
        assertThat(Version.parseVersion("1.1.0"), is(new UnitVersion((short) 1, (short) 1, (short) 0)));
        assertThat(Version.parseVersion("1.1.01"), is(new UnitVersion((short) 1, (short) 1, (short) 1)));
        assertThat(Version.parseVersion("1.1"), is(new UnitVersion((short) 1, (short) 1, (short) 0)));
        assertThat(Version.parseVersion("1"), is(new UnitVersion((short) 1, (short) 0, (short) 0)));
        assertThat(Version.parseVersion("latest"), is(Version.LATEST));
    }

    @Test
    public void testParseErrors() {
        Assertions.assertThrows(VersionParseException.class, () -> Version.parseVersion("1.1.1.1"));
        Assertions.assertThrows(VersionParseException.class, () -> Version.parseVersion(""));
        Assertions.assertThrows(VersionParseException.class, () -> Version.parseVersion("version"));
        Assertions.assertThrows(VersionParseException.class, () -> Version.parseVersion("1."));
        Assertions.assertThrows(VersionParseException.class, () -> Version.parseVersion(String.valueOf(Integer.MAX_VALUE)));
        Assertions.assertThrows(VersionParseException.class, () -> Version.parseVersion("1.1f"));
    }
}
