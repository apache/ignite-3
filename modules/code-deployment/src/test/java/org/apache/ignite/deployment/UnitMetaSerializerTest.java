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

import static org.apache.ignite.internal.deployunit.metastore.key.UnitMetaSerializer.deserialize;
import static org.apache.ignite.internal.deployunit.metastore.key.UnitMetaSerializer.serialize;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.apache.ignite.internal.deployunit.metastore.key.UnitMetaSerializer;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for {@link UnitMetaSerializer}.
 */
public class UnitMetaSerializerTest {
    private static List<Arguments> metaProvider() {
        return List.of(
                arguments("id", Version.LATEST, List.of("fileName"), UPLOADING, Arrays.asList("id1", "id2")),
                arguments("id", Version.LATEST, List.of("fileName1", "fileName2"), UPLOADING, Arrays.asList("id1", "id2")),
                arguments("id", Version.parseVersion("3.0.0"), List.of("fileName"), UPLOADING, Arrays.asList("id1", "id2")),
                arguments("id", Version.parseVersion("3.0"), List.of("fileName"), UPLOADING, Arrays.asList("id1", "id2")),
                arguments("id", Version.parseVersion("3.0.0"), List.of("fileName"), UPLOADING, Collections.emptyList()),
                arguments("id", Version.parseVersion("3.0.0"), List.of("fileName1", "fileName2"), UPLOADING, Collections.emptyList()),
                arguments("id;", Version.parseVersion("3.0.0"), List.of("fileName;"), UPLOADING, Collections.emptyList()),
                arguments("id;", Version.parseVersion("3.0.0"), List.of("fileName1:;", "fileName2"), UPLOADING, Collections.emptyList())
        );
    }

    @ParameterizedTest
    @MethodSource("metaProvider")
    public void testSerializeDeserialize(
            String id,
            Version version,
            List<String> fileNames,
            DeploymentStatus status,
            List<String> consistentIdLocation
    ) {
        UnitStatus meta = new UnitStatus(id, version, status);

        byte[] serialize = serialize(meta);

        assertThat(deserialize(serialize), is(meta));
    }
}
