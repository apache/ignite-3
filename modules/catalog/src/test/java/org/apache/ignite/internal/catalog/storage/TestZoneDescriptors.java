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

package org.apache.ignite.internal.catalog.storage;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;

/**
 * Random {@link CatalogZoneDescriptor}s for testing.
 */
final class TestZoneDescriptors {

    private static final List<CatalogStorageProfilesDescriptor> STORAGE_PROFILES = List.of(
            new CatalogStorageProfilesDescriptor(
                    List.of(new CatalogStorageProfileDescriptor("S1"))
            ),
            new CatalogStorageProfilesDescriptor(
                    List.of(new CatalogStorageProfileDescriptor("S1"), new CatalogStorageProfileDescriptor("S2"))
            )
    );

    /**
     * Generates a list of {@link CatalogZoneDescriptor}s of the given version.
     *
     * @param protocolVersion Protocol version.
     * @param state Random state,
     * @param version Version.
     * @return A list of descriptors.
     */
    static List<CatalogZoneDescriptor> zones(int protocolVersion, TestDescriptorState state, int version) {
        switch (version) {
            case 1:
                return zonesV0(state);
            case 2:
                // Protocol 2, version 1 and version 2 have different data but have the share same files.
                if (protocolVersion == 1) {
                    return zonesV0(state);
                } else {
                    return zonesV2(state);
                }
            default:
                throw new IllegalArgumentException("UnexpectedC atalogZoneDescriptor version: " + version);
        }
    }

    private static List<CatalogZoneDescriptor> zonesV0(TestDescriptorState state) {
        List<CatalogZoneDescriptor> list = new ArrayList<>();

        int defaultQuorumSize = 2;
        list.add(new CatalogZoneDescriptor(
                state.id(),
                state.name("ZONE"),
                1,
                2,
                defaultQuorumSize,
                3,
                4,
                5,
                CatalogUtils.DEFAULT_FILTER,
                STORAGE_PROFILES.get(0),
                ConsistencyMode.STRONG_CONSISTENCY
        ));

        list.add(new CatalogZoneDescriptor(
                state.id(),
                state.name("ZONE"),
                5,
                4,
                defaultQuorumSize,
                3,
                2,
                1,
                "$[?(@.region == \"europe\")]",
                STORAGE_PROFILES.get(0),
                ConsistencyMode.HIGH_AVAILABILITY
        ));

        list.add(new CatalogZoneDescriptor(state.id(), state.name("ZONE"),
                5,
                4,
                defaultQuorumSize,
                3,
                2,
                1,
                CatalogUtils.DEFAULT_FILTER,
                STORAGE_PROFILES.get(1),
                ConsistencyMode.HIGH_AVAILABILITY
        ));

        return list;
    }

    private static List<CatalogZoneDescriptor> zonesV2(TestDescriptorState state) {
        List<CatalogZoneDescriptor> list = new ArrayList<>();

        list.add(new CatalogZoneDescriptor(
                state.id(),
                state.name("ZONE"),
                1,
                2,
                2,
                4,
                5,
                CatalogUtils.DEFAULT_FILTER,
                STORAGE_PROFILES.get(0),
                ConsistencyMode.STRONG_CONSISTENCY
        ));

        list.add(new CatalogZoneDescriptor(
                state.id(),
                state.name("ZONE"),
                6,
                5,
                3,
                2,
                1,
                "$[?(@.region == \"europe\")]",
                STORAGE_PROFILES.get(0),
                ConsistencyMode.HIGH_AVAILABILITY
        ));

        list.add(new CatalogZoneDescriptor(
                state.id(),
                state.name("ZONE"),
                6,
                10,
                5,
                2,
                1,
                CatalogUtils.DEFAULT_FILTER,
                STORAGE_PROFILES.get(1),
                ConsistencyMode.HIGH_AVAILABILITY
        ));

        return list;
    }
}
