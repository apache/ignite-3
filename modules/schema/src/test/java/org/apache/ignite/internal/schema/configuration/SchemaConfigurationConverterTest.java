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

package org.apache.ignite.internal.schema.configuration;

import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableValidator;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.schema.builder.HashIndexBuilderTest;
import org.apache.ignite.internal.schema.builder.PartialIndexBuilderTest;
import org.apache.ignite.internal.schema.builder.PrimaryKeyBuilderTest;
import org.apache.ignite.internal.schema.builder.SchemaTableBuilderTest;
import org.apache.ignite.internal.schema.builder.SortedIndexBuilderTest;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.PartialIndex;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.SortedIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.apache.ignite.internal.schema.builder.SchemaTableBuilderTest.TBL;

/** SchmConfigurationConverter tests. */
public class SchemaConfigurationConverterTest {
    /** Configuration registry with one table for each test. */
    private ConfigurationRegistry confRegistry;

    /**
     * Prepare configuration registry for test.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @BeforeEach
    public void createRegistry() throws ExecutionException, InterruptedException {
        confRegistry = new ConfigurationRegistry(
            Collections.singleton(TablesConfiguration.KEY),
            Collections.singletonMap(TableValidator.class, Collections.singleton(SchemaTableValidatorImpl.INSTANCE)),
            Collections.singleton(new TestConfigurationStorage()));

        confRegistry.getConfiguration(TablesConfiguration.KEY).change(
            ch -> {
                SchemaConfigurationConverter.createTable(TBL, ch);
                ch.changeTables(tblsCh -> tblsCh.create(TBL.canonicalName(),
                    tblCh -> tblCh.changeReplicas(1)));
            }).get();
    }

    /**
     * Add/remove HashIndex into configuration and read it back.
     */
    @Test
    public void testConvertHashIndex() throws ExecutionException, InterruptedException {
        HashIndex idx = HashIndexBuilderTest.IDX;
        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();
    }

    /**
     * Add/remove SortedIndex into configuration and read it back.
     */
    @Test
    public void testConvertSortedIndex() throws ExecutionException, InterruptedException {
        SortedIndex idx = SortedIndexBuilderTest.IDX;

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();
    }

    /**
     * Add/remove PrimaryIndex into configuration and read it back.
     */
    @Test
    public void testPKIndex() throws ExecutionException, InterruptedException {
        PrimaryIndex idx = PrimaryKeyBuilderTest.IDX;

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();
    }

    /**
     * Add/remove PartialIndex into configuration and read it back.
     */
    @Test
    public void testPartialIndex() throws ExecutionException, InterruptedException {
        PartialIndex idx = PartialIndexBuilderTest.IDX;

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();
    }

    /**
     * Add/remove table and read it back.
     */
    @Test
    public void testConvertTable() {
        // TODO
    }

    /**
     * Get tests default table configuration.
     * @return Configuration of default table.
     */
    private TableConfiguration getTbl() {
        return confRegistry.getConfiguration(TablesConfiguration.KEY).tables().get(TBL.canonicalName());
    }
}
