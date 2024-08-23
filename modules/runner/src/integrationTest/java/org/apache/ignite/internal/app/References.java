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

package org.apache.ignite.internal.app;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.TEST_TABLE_NAME;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.PartitionManager;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * References to API objects extracted from an {@link IgniteServer} instance.
 */
class References {
    final Ignite ignite;

    final IgniteTables tables;
    final IgniteTransactions transactions;

    final Table table; // From table().
    final Table tableFromTableAsync;
    final Table tableFromTables;
    final Table tableFromTablesAsync;

    final KeyValueView<Tuple, Tuple> kvView;
    final KeyValueView<Integer, String> typedKvView;
    final KeyValueView<Integer, String> mappedKvView;

    final RecordView<Tuple> recordView;
    final RecordView<Record> typedRecordView;
    final RecordView<Record> mappedRecordView;

    final PartitionManager partitionManager;

    References(IgniteServer server) throws Exception {
        ignite = server.api();

        tables = ignite.tables();
        transactions = ignite.transactions();

        table = tables.table(TEST_TABLE_NAME);
        tableFromTableAsync = tables.tableAsync(TEST_TABLE_NAME).get(10, SECONDS);
        tableFromTables = tables.tables().get(0);
        tableFromTablesAsync = tables.tablesAsync().get(10, SECONDS).get(0);

        kvView = table.keyValueView();
        typedKvView = table.keyValueView(Integer.class, String.class);
        mappedKvView = table.keyValueView(Mapper.of(Integer.class), Mapper.of(String.class));

        recordView = table.recordView();
        typedRecordView = table.recordView(Record.class);
        mappedRecordView = table.recordView(Mapper.of(Record.class));

        partitionManager = table.partitionManager();
    }
}
