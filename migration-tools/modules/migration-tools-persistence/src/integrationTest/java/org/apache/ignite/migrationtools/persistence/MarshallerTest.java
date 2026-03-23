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

package org.apache.ignite.migrationtools.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

@ExtendWith(FullSampleCluster.class)
class MarshallerTest {
    private static final List<String> CACHE_NAMES = List.of(
            "MySimpleMap",
            "MyPersonPojoCache",
            "MyOrganizations",
            "MyIntArrCache",
            "MyListArrCache",
            "MyBinaryPersonPojoCache",
            "MyBinaryOrganizationCache",
            "MyBinaryTestCache"
    );

    @ExtendWith(BasePersistentTestContext.class)
    private List<MigrationKernalContext> nodeContexts;

    private MigrationKernalContext nodeCtx;

    @BeforeEach
    void beforeEach() throws IgniteCheckedException {
        nodeCtx = nodeContexts.get(0);
        nodeCtx.start();
    }

    @Test
    void loadAllTest() throws IgniteCheckedException {
        ((MigrationCacheProcessor) nodeCtx.cache()).loadAllDescriptors();

        Collection<DynamicCacheDescriptor> descriptors = nodeCtx.cache().persistentCaches().stream()
                .filter(c -> c.cacheType() == CacheType.USER)
                .collect(Collectors.toList());

        assertThat(descriptors).hasSize(8);

        assertThat(descriptors)
                .map(DynamicCacheDescriptor::cacheConfiguration)
                .doesNotContainNull();
    }

    @ParameterizedTest
    @FieldSource("CACHE_NAMES")
    void loadEachTest(String cacheName) {
        var cacheDescriptor = nodeCtx.cache().cacheDescriptor(cacheName);
        assertThat(cacheDescriptor).isNotNull();
        assertThat(cacheDescriptor.cacheConfiguration()).isNotNull();
    }
}
