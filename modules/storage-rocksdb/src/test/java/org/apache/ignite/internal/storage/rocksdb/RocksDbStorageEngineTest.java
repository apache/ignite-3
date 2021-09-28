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

package org.apache.ignite.internal.storage.rocksdb;

import java.nio.file.Path;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbStorageEngineTest {
    /** Tests that roccksdb engine creates tables of proper type. */
    @Test
    public void test(
        @WorkDirectory Path workDir,
        @InjectConfiguration DataRegionConfiguration dataRegionCfg,
        @InjectConfiguration TableConfiguration tableCfg
    ) throws Exception {
        dataRegionCfg.change(cfg -> cfg.changeSize(16 * 1024).changeWriteBufferSize(16 * 1024)).get();

        RocksDbStorageEngine engine = new RocksDbStorageEngine();

        DataRegion dataRegion = engine.createDataRegion(dataRegionCfg);

        try {
            assertThat(dataRegion, is(instanceOf(RocksDbDataRegion.class)));

            dataRegion.start();

            TableStorage table = engine.createTable(workDir, tableCfg, dataRegion, (tableView, indexName) -> null);

            try {
                assertThat(table, is(instanceOf(RocksDbTableStorage.class)));

                table.start();

                Storage partition = table.getOrCreatePartition(0);

                assertThat(partition, is(instanceOf(RocksDbStorage.class)));

                partition.write(new SimpleDataRow(new byte[4], new byte[0]));
            }
            finally {
                table.stop();
            }
        }
        finally {
            dataRegion.stop();
        }
    }
}