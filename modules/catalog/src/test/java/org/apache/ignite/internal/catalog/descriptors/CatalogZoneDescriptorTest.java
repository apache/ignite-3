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

package org.apache.ignite.internal.catalog.descriptors;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

import java.util.List;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.junit.jupiter.api.Test;

class CatalogZoneDescriptorTest {
    @Test
    void toStringContainsTypeAndFields() {
        var descriptor = new CatalogZoneDescriptor(
                1,
                "zone1",
                2,
                3,
                4,
                5,
                6,
                "the-filter",
                fromParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
        );

        String toString = descriptor.toString();

        assertThat(toString, startsWith("CatalogZoneDescriptor ["));
        assertThat(toString, containsString("id=1"));
        assertThat(toString, containsString("name=zone1"));
        assertThat(toString, containsString("partitions=2"));
        assertThat(toString, containsString("replicas=3"));
        assertThat(toString, containsString("dataNodesAutoAdjust=4"));
        assertThat(toString, containsString("dataNodesAutoAdjustScaleUp=5"));
        assertThat(toString, containsString("dataNodesAutoAdjustScaleDown=6"));
        assertThat(toString, containsString("filter=the-filter"));
        assertThat(toString, containsString("storageProfiles=CatalogStorageProfilesDescriptor ["));
    }
}
