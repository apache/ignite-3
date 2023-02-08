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

package org.apache.ignite.deployment;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.UnitMeta;
import org.apache.ignite.internal.deployunit.UnitMetaSerializer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link UnitMetaSerializer}.
 */
public class UnitMetaSerializerTest {

    @Test
    public void testSerializeDeserializeLatest() {
        UnitMeta meta = new UnitMeta("id", Version.LATEST, "unitName", Arrays.asList("id1", "id2"));

        byte[] serialize = UnitMetaSerializer.serialize(meta);

        MatcherAssert.assertThat(UnitMetaSerializer.deserialize(serialize), Matchers.is(meta));
    }

    @Test
    public void testSerializeDeserializeUnit() {
        UnitMeta meta = new UnitMeta("id", Version.parseVersion("3.0.0"), "unitName", Arrays.asList("id1", "id2"));

        byte[] serialize = UnitMetaSerializer.serialize(meta);

        MatcherAssert.assertThat(UnitMetaSerializer.deserialize(serialize), Matchers.is(meta));
    }

    @Test
    public void testSerializeDeserializeUnitIncompleteVersion() {
        UnitMeta meta = new UnitMeta("id", Version.parseVersion("3.0"), "unitName", Arrays.asList("id1", "id2"));

        byte[] serialize = UnitMetaSerializer.serialize(meta);

        MatcherAssert.assertThat(UnitMetaSerializer.deserialize(serialize), Matchers.is(meta));
    }

    @Test
    public void testSerializeDeserializeUnitEmptyConsistentId() {
        UnitMeta meta = new UnitMeta("id", Version.parseVersion("3.0.0"), "unitName", Collections.emptyList());

        byte[] serialize = UnitMetaSerializer.serialize(meta);

        UnitMeta deserialize = UnitMetaSerializer.deserialize(serialize);
        MatcherAssert.assertThat(deserialize, Matchers.is(meta));
    }
}
