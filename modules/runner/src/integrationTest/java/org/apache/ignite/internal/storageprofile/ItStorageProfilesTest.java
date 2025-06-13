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

package org.apache.ignite.internal.storageprofile;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.typesafe.config.parser.ConfigDocument;
import com.typesafe.config.parser.ConfigDocumentFactory;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileView;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileView;
import org.junit.jupiter.api.Test;

/**
 * Test class for validating storage profiles configuration.
 */
public class ItStorageProfilesTest extends ClusterPerClassIntegrationTest {
    private static final String AIPERSIST_PROFILE_NAME = ItStorageProfilesTest.class.getSimpleName() + "_aipersist";
    private static final String AIMEM_PROFILE_NAME = ItStorageProfilesTest.class.getSimpleName() + "_aimem";
    private static final String ROCKSDB_PROFILE_NAME = ItStorageProfilesTest.class.getSimpleName() + "_rocksdb";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        ConfigDocument document = ConfigDocumentFactory.parseString(super.getNodeBootstrapConfigTemplate());

        document = document.withValueText(profileKey(ROCKSDB_PROFILE_NAME), RocksDbStorageEngine.ENGINE_NAME);

        document = document.withValueText(profileKey(AIPERSIST_PROFILE_NAME), PersistentPageMemoryStorageEngine.ENGINE_NAME);

        document = document.withValueText(profileKey(AIMEM_PROFILE_NAME), VolatilePageMemoryStorageEngine.ENGINE_NAME);

        return document.render();
    }

    private static String profileKey(String profileName) {
        return String.format("ignite.storage.profiles.%s.engine", profileName);
    }

    /**
     * Checks that profiles with default values get initialized correctly.
     */
    @Test
    void validateDefaultStorageProfileSettings() {
        long defaultDataRegionSize = StorageEngine.defaultDataRegionSize();

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        NamedListView<StorageProfileView> profilesConfiguration = node.nodeConfiguration()
                .getConfiguration(StorageExtensionConfiguration.KEY)
                .storage()
                .profiles()
                .value();

        var aipersistConfiguration = (PersistentPageMemoryProfileView) profilesConfiguration.get(AIPERSIST_PROFILE_NAME);

        assertThat(aipersistConfiguration, is(notNullValue()));
        assertThat(aipersistConfiguration.sizeBytes(), is(defaultDataRegionSize));

        var aimemConfiguration = (VolatilePageMemoryProfileView) profilesConfiguration.get(AIMEM_PROFILE_NAME);

        assertThat(aimemConfiguration, is(notNullValue()));
        assertThat(aimemConfiguration.initSizeBytes(), is(defaultDataRegionSize));
        assertThat(aimemConfiguration.maxSizeBytes(), is(defaultDataRegionSize));

        var rocksdbConfiguration = (RocksDbProfileView) profilesConfiguration.get(ROCKSDB_PROFILE_NAME);

        assertThat(rocksdbConfiguration, is(notNullValue()));
        assertThat(rocksdbConfiguration.sizeBytes(), is(defaultDataRegionSize));
    }
}
