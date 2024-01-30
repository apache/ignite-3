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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageChange;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/** Data storage manager. */
public class DataStorageManager implements IgniteComponent {
    /** Mapping: {@link DataStorageModule#name} -> {@link StorageEngine}. */
    private final Map<String, StorageEngine> engines;

    /**
     * Constructor.
     *
     * @param engines Storage engines unique by {@link DataStorageModule#name name}.
     */
    public DataStorageManager(Map<String, StorageEngine> engines) {
        assert !engines.isEmpty();

        this.engines = engines;
    }

    @Override
    public CompletableFuture<Void> start() throws StorageException {
        engines.values().forEach(StorageEngine::start);

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        IgniteUtils.closeAll(engines.values().stream().map(engine -> engine::stop));
    }

    /**
     * Returns the data storage engine by name, {@code null} if absent.
     *
     * @param name Storage engine name.
     */
    public @Nullable StorageEngine engine(String name) {
        return engines.get(name);
    }

    /**
     * Creates a consumer that will change the {@link DataStorageConfigurationSchema data storage}.
     *
     * @param dataStorage Data storage.
     * @param values {@link Value Values} for the data storage. Mapping: field name -> field value.
     */
    // TODO: IGNITE-20263 Get rid of
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

    @Override
    public String toString() {
        return S.toString(DataStorageManager.class, this);
    }
}
