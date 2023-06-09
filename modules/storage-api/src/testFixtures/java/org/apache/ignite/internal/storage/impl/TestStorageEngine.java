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

package org.apache.ignite.internal.storage.impl;

import static org.mockito.Mockito.spy;

import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;

/**
 * Test implementation of the {@link StorageEngine} based on class {@link ConcurrentSkipListMap}.
 */
public class TestStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "test";

    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public void start() throws StorageException {
        // No-op.
    }

    @Override
    public void stop() throws StorageException {
        // No-op.
    }

    @Override
    public MvTableStorage createMvTable(TableConfiguration tableCfg, TablesConfiguration tablesCfg,
            DistributionZoneConfiguration distributionZoneCfg)
            throws StorageException {
        String dataStorageName = distributionZoneCfg.dataStorage().name().value();

        assert dataStorageName.equals(ENGINE_NAME) : dataStorageName;

        return spy(new TestMvTableStorage(tableCfg.id().value(), distributionZoneCfg.partitions().value()));
    }
}
