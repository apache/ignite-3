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

package org.apache.ignite.internal.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.configuration.ConfigurationDefaultsPatcher;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;

/**
 * Implementation of {@link ConfigurationDefaultsPatcher}.
 */
public class ClusterConfigurationDefaultsPatcherImpl implements ConfigurationDefaultsPatcher {
    /**
     * Configuration module.
     */
    private final ConfigurationModule configurationModule;

    /**
     * Configuration tree generator.
     */
    private final ConfigurationTreeGenerator generator;


    public ClusterConfigurationDefaultsPatcherImpl(
            ConfigurationModule configurationModule,
            ConfigurationTreeGenerator generator
    ) {
        this.configurationModule = configurationModule;
        this.generator = generator;
    }

    @Override
    public String patchDefaults(String hocon) {
        SuperRoot superRoot = convertToSuperRoot(hocon);

        SuperRootChange rootChange = new SuperRootChange() {
            @Override
            public <V> V viewRoot(RootKey<? extends ConfigurationTree<V, ?>, V> rootKey) {
                return Objects.requireNonNull(superRoot.getRoot(rootKey)).specificNode();
            }

            @Override
            public <C> C changeRoot(RootKey<? extends ConfigurationTree<?, C>, ?> rootKey) {
                // "construct" does a field copying, which is what we need before mutating it.
                superRoot.construct(rootKey.key(), ConfigurationUtil.EMPTY_CFG_SRC, true);

                // "rootView" is not re-used here because of return type incompatibility, although code is the same.
                return Objects.requireNonNull(superRoot.getRoot(rootKey)).specificNode();
            }
        };

        configurationModule.patchDefaults(rootChange);

        ConverterToMapVisitor visitor = ConverterToMapVisitor.builder()
                .includeInternal(true)
                .maskSecretValues(false)
                .build();

        return HoconConverter.represent(superRoot, List.of(), visitor).render(ConfigRenderOptions.concise());
    }

    private SuperRoot convertToSuperRoot(String hocon) {
        Config config = ConfigFactory.parseString(hocon);
        ConfigurationSource hoconSource = HoconConverter.hoconSource(config.root());

        SuperRoot superRoot = generator.createEmptySuperRoot();
        hoconSource.descend(superRoot);

        return superRoot;
    }
}
