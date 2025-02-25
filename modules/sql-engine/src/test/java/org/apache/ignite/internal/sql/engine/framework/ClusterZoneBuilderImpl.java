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

package org.apache.ignite.internal.sql.engine.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.ClusterBuilder;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.ClusterBuilderImpl;

/**
 * An implementation of the {@link ClusterZoneBuilder} interface.
 *
 * <p>
 * This class provides the concrete functionality for building and configuring a test zone nested within a cluster.
 * It enables the creation of zones through a fluent API, adhering to the contract defined by {@link ClusterZoneBuilder}.
 * </p>
 *
 * @see ClusterZoneBuilder
 */
class ClusterZoneBuilderImpl implements ClusterZoneBuilder {
    private final ClusterBuilderImpl parent;

    private String name;

    private List<String> storageProfiles;

    ClusterZoneBuilderImpl(ClusterBuilderImpl parent) {
        this.parent = parent;
    }

    @Override
    public ClusterBuilder end() {
        parent.zoneBuilders().add(this);

        return parent;
    }

    @Override
    public ClusterZoneBuilder name(String name) {
        this.name = name;

        return this;
    }

    @Override
    public ClusterZoneBuilder storageProfiles(List<String> storageProfiles) {
        this.storageProfiles = new ArrayList<>(storageProfiles);

        return this;
    }

    @Override
    public CatalogCommand build() {
        List<StorageProfileParams> storageProfileParams = storageProfiles.stream()
                .map(profileName -> StorageProfileParams.builder().storageProfile(profileName).build())
                .collect(Collectors.toList());

        return CreateZoneCommand.builder()
                .zoneName(name)
                .storageProfilesParams(storageProfileParams)
                .build();
    }
}
