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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/** Data storage manager. */
public class DataStorageManager implements IgniteComponent {
    // TODO: IGNITE-20237 Make it configurable
    private static final String DEFAULT_DATA_STORAGE = "aipersist";

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
    public void start() throws StorageException {
        engines.values().forEach(StorageEngine::start);
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

    /** Returns the default data storage. */
    public static String defaultDataStorage() {
        return DEFAULT_DATA_STORAGE;
    }

    @Override
    public String toString() {
        return S.toString(DataStorageManager.class, this);
    }
}
