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

package org.apache.ignite.internal.storage;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageChange;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Data storage manager.
 */
public class DataStorageManager implements IgniteComponent {
    private final ConfigurationValue<String> defaultDataStorageConfig;

    /** Mapping: {@link DataStorageModule#name} -> {@link StorageEngine}. */
    private final Map<String, StorageEngine> engines;

    /**
     * Constructor.
     *
     * @param dstZnsCfg Zones configuration.
     * @param engines Storage engines unique by {@link DataStorageModule#name name}.
     */
    public DataStorageManager(
            DistributionZonesConfiguration dstZnsCfg,
            Map<String, StorageEngine> engines
    ) {
        assert !engines.isEmpty();

        this.engines = engines;

        defaultDataStorageConfig = dstZnsCfg.defaultDataStorage();
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        engines.values().forEach(StorageEngine::start);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        IgniteUtils.closeAll(engines.values().stream().map(engine -> engine::stop));
    }

    /**
     * Returns the data storage engine by data storage configuration.
     *
     * @param config Data storage configuration.
     */
    public @Nullable StorageEngine engine(DataStorageConfiguration config) {
        return engines.get(config.value().name());
    }

    /**
     * Returns a consumer that will set the default {@link DistributionZoneConfiguration#dataStorage table data storage}
     * depending on the {@link StorageEngine engine}.
     *
     * @param defaultDataStorageView View of {@link DistributionZonesConfiguration#defaultDataStorage}.
     */
    public Consumer<DataStorageChange> defaultZoneDataStorageConsumer(String defaultDataStorageView) {
        return zoneDataStorageChange -> {
            assert engines.containsKey(defaultDataStorageView)
                    : "Default Storage Engine \"" + defaultDataStorageView + "\" is missing from configuration";

            zoneDataStorageChange.convert(defaultDataStorageView);
        };
    }

    /**
     * Returns the default data storage.
     *
     * <p>{@link DistributionZonesConfiguration#defaultDataStorage} is used.
     */
    public String defaultDataStorage() {
        return defaultDataStorageConfig.value();
    }

    /**
     * Creates a consumer that will change the {@link DataStorageConfigurationSchema data storage} for the {@link
     * DistributionZoneConfiguration#dataStorage}.
     *
     * @param dataStorage Data storage.
     * @param values {@link Value Values} for the data storage. Mapping: field name -> field value.
     */
    public Consumer<DataStorageChange> zoneDataStorageConsumer(String dataStorage, Map<String, Object> values) {
        ConfigurationSource configurationSource = new ConfigurationSource() {
            /** {@inheritDoc} */
            @Override
            public String polymorphicTypeId(String fieldName) {
                throw new UnsupportedOperationException("polymorphicTypeId");
            }

            /** {@inheritDoc} */
            @Override
            public void descend(ConstructableTreeNode node) {
                for (Entry<String, Object> e : values.entrySet()) {
                    assert e.getKey() != null;
                    assert e.getValue() != null : e.getKey();

                    ConfigurationSource leafSource = new ConfigurationSource() {
                        /** {@inheritDoc} */
                        @Override
                        public <T> T unwrap(Class<T> clazz) {
                            return clazz.cast(e.getValue());
                        }

                        /** {@inheritDoc} */
                        @Override
                        public void descend(ConstructableTreeNode node) {
                            throw new UnsupportedOperationException("descend");
                        }

                        /** {@inheritDoc} */
                        @Override
                        public String polymorphicTypeId(String fieldName) {
                            throw new UnsupportedOperationException("polymorphicTypeId");
                        }
                    };

                    node.construct(e.getKey(), leafSource, true);
                }
            }
        };

        return zoneDataStorageChange -> {
            zoneDataStorageChange.convert(dataStorage);

            configurationSource.descend((ConstructableTreeNode) zoneDataStorageChange);
        };
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(DataStorageManager.class, this);
    }
}
