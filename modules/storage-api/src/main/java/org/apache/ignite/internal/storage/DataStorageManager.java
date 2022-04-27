/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema.UNKNOWN_DATA_STORAGE;
import static org.apache.ignite.internal.util.CollectionUtils.first;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.store.DataStorageChange;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfigurationSchema;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.manager.IgniteComponent;
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
     * @param tablesConfig Tables configuration.
     * @param engines Storage engines unique by {@link DataStorageModule#name name}.
     */
    public DataStorageManager(
            TablesConfiguration tablesConfig,
            Map<String, StorageEngine> engines
    ) {
        assert !engines.isEmpty();

        this.engines = engines;

        defaultDataStorageConfig = tablesConfig.defaultDataStorage();
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
     * Returns a consumer that will set the default {@link TableConfigurationSchema#dataStorage table data storage} depending on the {@link
     * StorageEngine engine}.
     *
     * @param defaultDataStorageView View of {@link TablesConfigurationSchema#defaultDataStorage}. For the case {@link
     *      UnknownDataStorageConfigurationSchema#UNKNOWN_DATA_STORAGE} and there is only one engine, then it will be the default, otherwise
     *      there will be no default.
     */
    // TODO: IGNITE-16835 Remove it.
    public Consumer<DataStorageChange> defaultTableDataStorageConsumer(String defaultDataStorageView) {
        return tableDataStorageChange -> {
            if (!defaultDataStorageView.equals(UNKNOWN_DATA_STORAGE)) {
                assert engines.containsKey(defaultDataStorageView) : defaultDataStorageView;

                tableDataStorageChange.convert(defaultDataStorageView);
            } else if (engines.size() == 1) {
                tableDataStorageChange.convert(first(engines.keySet()));
            }
        };
    }

    /**
     * Returns the default data storage.
     *
     * <p>{@link TablesConfigurationSchema#defaultDataStorage} is used. For the case {@link
     * UnknownDataStorageConfigurationSchema#UNKNOWN_DATA_STORAGE} and there is only one engine, then it will be the default.
     */
    public String defaultDataStorage() {
        String defaultDataStorage = defaultDataStorageConfig.value();

        return !defaultDataStorage.equals(UNKNOWN_DATA_STORAGE) || engines.size() > 1 ? defaultDataStorage : first(engines.keySet());
    }

    /**
     * Creates a consumer that will change the {@link DataStorageConfigurationSchema data storage} for the {@link
     * TableConfigurationSchema#dataStorage}.
     *
     * @param dataStorage Data storage, {@link UnknownDataStorageConfigurationSchema#UNKNOWN_DATA_STORAGE} is invalid.
     * @param values {@link Value Values} for the data storage. Mapping: field name -> field value.
     */
    public Consumer<DataStorageChange> tableDataStorageConsumer(String dataStorage, Map<String, Object> values) {
        assert !dataStorage.equals(UNKNOWN_DATA_STORAGE);

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

        return tableDataStorageChange -> {
            tableDataStorageChange.convert(dataStorage);

            configurationSource.descend((ConstructableTreeNode) tableDataStorageChange);
        };
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(DataStorageManager.class, this);
    }
}
