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

package org.apache.ignite.internal.deployunit.metastore.status;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import java.util.List;
import org.apache.ignite.deployment.version.Version;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class UnitKeyTest {
    private static List<NodeStatusKey> nodeKeys() {
        return List.of(
                NodeStatusKey.builder().build(),
                NodeStatusKey.builder().id("id").build(),
                NodeStatusKey.builder().id("id").version(Version.LATEST).build(),
                NodeStatusKey.builder().id("id").version(Version.LATEST).nodeId("nodeId").build()
        );
    }

    private static List<ClusterStatusKey> clusterKeys() {
        return List.of(
                ClusterStatusKey.builder().build(),
                ClusterStatusKey.builder().id("id").build(),
                ClusterStatusKey.builder().id("id").version(Version.LATEST).build()
        );
    }

    @ParameterizedTest
    @MethodSource("nodeKeys")
    void nodeStatusKey(NodeStatusKey key) {
        assertThat(NodeStatusKey.fromBytes(key.toByteArray().bytes()), is(key));
    }

    @ParameterizedTest
    @MethodSource("clusterKeys")
    void clusterStatusKey(ClusterStatusKey key) {
        assertThat(ClusterStatusKey.fromBytes(key.toByteArray().bytes()), is(key));
    }

    @Test
    void prefix() {
        ClusterStatusKey all = ClusterStatusKey.builder().build();
        ClusterStatusKey id = ClusterStatusKey.builder().id("test-unit").build();
        ClusterStatusKey version = ClusterStatusKey.builder().id("test-unit").version(Version.LATEST).build();
        ClusterStatusKey id2 = ClusterStatusKey.builder().id("test-unit2").build();

        String strAll = all.toByteArray().toString();
        String str = id.toByteArray().toString();
        String strVersion = version.toByteArray().toString();
        String str2 = id2.toByteArray().toString();

        // All keys starts with the prefix
        assertThat(str, is(startsWith(strAll)));
        assertThat(str2, is(startsWith(strAll)));
        assertThat(strVersion, is(startsWith(strAll)));

        // Key with id and version starts with the key with id
        assertThat(strVersion, is(startsWith(str)));

        // Key with longer id doesn't start with key with shorter id
        assertThat(str2, is(not(startsWith(str))));
    }
}
