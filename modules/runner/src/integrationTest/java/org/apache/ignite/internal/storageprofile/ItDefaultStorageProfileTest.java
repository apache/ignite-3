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

import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;

import com.typesafe.config.parser.ConfigDocument;
import com.typesafe.config.parser.ConfigDocumentFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbTableStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.junit.jupiter.api.Test;

class ItDefaultStorageProfileTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        ConfigDocument document = ConfigDocumentFactory.parseString(super.getNodeBootstrapConfigTemplate());
        document = document.withValueText("ignite.storage.profiles." + CatalogService.DEFAULT_STORAGE_PROFILE + ".engine", "rocksdb");
        return document.render();
    }

    @Test
    void engineInDefaultStorageProfileCanBeChanged() {
        Ignite node = node(0);

        node.sql().executeScript("CREATE TABLE TEST_TABLE (ID INT PRIMARY KEY)");

        TableImpl table = unwrapTableImpl(node.tables().table("TEST_TABLE"));
        MvTableStorage tableStorage = Wrappers.unwrap(table.internalTable().storage(), MvTableStorage.class);

        assertThat(tableStorage, isA(RocksDbTableStorage.class));
    }
}
