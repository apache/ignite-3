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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.schema.SchemaBuilders.column;
import static org.apache.ignite.schema.SchemaBuilders.primaryKey;
import static org.apache.ignite.schema.SchemaBuilders.tableBuilder;
import static org.apache.ignite.schema.definition.ColumnType.INT32;
import static org.apache.ignite.schema.definition.ColumnType.string;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.IntFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Restart one node test.
 */
public class ItRestartTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override
    protected int nodes() {
        return 4;
    }

    /**
     * The test.
     */
    @Test
    public void test() {
        createTableWithData(CLUSTER_NODES.get(0), "t1", String::valueOf);

        String igniteName = CLUSTER_NODES.get(nodes() - 1).name();

        log.info("Stopping the node.");

        IgnitionManager.stop(igniteName);

        IntFunction<String> valueProducer = String::valueOf;

        createTableWithData(CLUSTER_NODES.get(0), "t2", valueProducer);

        log.info("Starting the node.");

        Ignite newNode = IgnitionManager.start(igniteName, null, WORK_DIR.resolve(igniteName));

        CLUSTER_NODES.set(nodes() - 1, newNode);

        checkTableWithData(CLUSTER_NODES.get(0), "t1", valueProducer);
        checkTableWithData(CLUSTER_NODES.get(0), "t1", valueProducer);

        checkTableWithData(CLUSTER_NODES.get(nodes() - 1), "t1", valueProducer);
        checkTableWithData(CLUSTER_NODES.get(nodes() - 1), "t2", valueProducer);
    }

    /**
     * Creates a table and load data to it.
     *
     * @param ignite Ignite.
     */
    private void createTableWithData(Ignite ignite, String name, IntFunction<String> valueProducer) {
        TableDefinition scmTbl1 = tableBuilder("PUBLIC", name).columns(
                column("id", INT32).build(),
                column("name", string()).asNullable(true).build()
        ).withPrimaryKey(
                primaryKey().withColumns("id").build()
        ).build();

        Table table = ignite.tables().createTable(
                scmTbl1.canonicalName(),
                tbl -> convert(scmTbl1, tbl).changePartitions(10).changeReplicas(nodes())
        );

        for (int i = 0; i < 1; i++) {
            Tuple key = Tuple.create().set("id", i);
            Tuple val = Tuple.create().set("name", valueProducer.apply(i));

            table.keyValueView().put(null, key, val);
        }
    }

    /**
     * Checks the table exists and validates all data in it.
     *
     * @param ignite        Ignite.
     * @param valueProducer Producer to predict a value.
     */
    private void checkTableWithData(Ignite ignite, String name, IntFunction<String> valueProducer) {
        Table table = ignite.tables().table("PUBLIC." + name);

        assertNotNull(table);

        for (int i = 0; i < 1; i++) {
            Tuple row = table.keyValueView().get(null, Tuple.create().set("id", i));

            assertEquals(valueProducer.apply(i), row.stringValue("name"));
        }
    }
}
