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

import static org.apache.ignite.internal.catalog.storage.TestCatalogObjectDescriptors.STORAGE_PROFILES;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;

/**
 * Tests for catalog storage objects. Protocol version 2 reads protocol 2.
 */
public class CatalogSerializationCompatibilityV2ReadsV2Test extends CatalogSerializationCompatibilityTest {

    @Override
    protected int protocolVersion() {
        return 2;
    }

    @Override
    protected int entryVersion() {
        return 2;
    }

    @Override
    protected String dirName() {
        return "serialization_v2";
    }

    @Override
    protected boolean expectExactVersion() {
        return true;
    }

    @Override
    protected List<CatalogZoneDescriptor> zones() {
        return zonesV2();
    }

    private List<CatalogZoneDescriptor> zonesV2() {
        List<CatalogZoneDescriptor> list = new ArrayList<>();

        list.add(new CatalogZoneDescriptor(
                state.id(),
                state.name("ZONE"),
                1,
                2,
                2,
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
                6,
                5,
                3,
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
                3,
                2,
                1,
                CatalogUtils.DEFAULT_FILTER,
                STORAGE_PROFILES.get(1),
                ConsistencyMode.HIGH_AVAILABILITY
        ));

        return list;
    }
}
