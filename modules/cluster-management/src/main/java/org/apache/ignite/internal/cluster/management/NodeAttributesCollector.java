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

package org.apache.ignite.internal.cluster.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributeView;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;

/**
 * This class is responsible for retrieving local node attributes
 * from system components before the local node joins the cluster.
 */
public class NodeAttributesCollector implements NodeAttributes {
    private final List<NodeAttributesProvider> systemAttributesProviders = new ArrayList<>();

    private final NodeAttributesConfiguration nodeAttributesConfiguration;

    private final StorageConfiguration storageProfilesConfiguration;

    public NodeAttributesCollector(
            NodeAttributesConfiguration nodeAttributesConfiguration,
            StorageConfiguration storageProfilesConfiguration
    ) {
        this.nodeAttributesConfiguration = nodeAttributesConfiguration;
        this.storageProfilesConfiguration = storageProfilesConfiguration;
    }

    /**
     * Registers system attributes provider.
     */
    public void register(NodeAttributesProvider provider) {
        systemAttributesProviders.add(provider);
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, String> userAttributes() {
        NamedListView<NodeAttributeView> attributes = nodeAttributesConfiguration.nodeAttributes().value();

        return attributes.stream()
                .collect(Collectors.toUnmodifiableMap(NodeAttributeView::name, NodeAttributeView::attribute));
    }

    /** {@inheritDoc} */
    @Override
    public List<String> storageProfiles() {
        NamedListView<StorageProfileView> storageProfiles = storageProfilesConfiguration.profiles().value();

        return storageProfiles.stream()
                .map(StorageProfileView::name)
                .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, String> systemAttributes() {
        Map<String, String> attributes = new HashMap<>();

        for (NodeAttributesProvider provider : systemAttributesProviders) {
            attributes.putAll(provider.nodeAttributes());
        }

        return attributes;
    }
}
