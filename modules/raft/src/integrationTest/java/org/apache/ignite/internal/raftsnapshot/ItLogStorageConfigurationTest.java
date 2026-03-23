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

package org.apache.ignite.internal.raftsnapshot;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.ConfigOverride;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyOptions;

class ItLogStorageConfigurationTest extends ClusterPerTestIntegrationTest {
    private Ignite node;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void prepare() {
        node = cluster.node(0);
    }

    @Test
    @ConfigOverride(name = "ignite.system.properties.partitionsRaftLogStorageWriteBufferSize", value = "" + (99L * 1024 * 1024))
    void partitionsLogStorageWriteBufferSizeIsTakenFromConfiguration() throws Exception {
        LogStorageManager logStorageManager = unwrapIgniteImpl(node).partitionsLogStorageManager();
        DefaultLogStorageManager defaultLogStorageManager = (DefaultLogStorageManager) logStorageManager;

        @SuppressWarnings("resource")
        ColumnFamilyOptions cfOptions = defaultLogStorageManager.dataColumnFamilyHandle().getDescriptor().getOptions();

        assertThat(cfOptions.writeBufferSize(), is(99L * 1024 * 1024));
    }
}
