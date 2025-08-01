package org.apache.ignite.example.cache;

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

import java.sql.Types;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.gridgain.cache.store.jdbc.JdbcCacheStoreFactory;
import org.gridgain.cache.store.jdbc.JdbcType;
import org.gridgain.cache.store.jdbc.JdbcTypeField;
import org.gridgain.cache.store.jdbc.dialect.H2Dialect;
import org.h2.jdbcx.JdbcConnectionPool;


/**
 * This example demonstrates how to create, read and write to Caches with GridGain 9, JDBC and external database H2 */

public class CacheExample {
    public static void main(String[] args) throws Exception {

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .cache.build()) {
            var dataSrc = JdbcConnectionPool.create("jdbc:h2:mem:GridGainKeyValueViewExample", "sa", "");

            /* Set up JDBC cache store factory */
            var storeFactory = new JdbcCacheStoreFactory();
            storeFactory.setDialect(new H2Dialect());

            /* Define table mapping */
            var jdbcType = getJdbcTypeMapping();
            storeFactory.setType(jdbcType);
            storeFactory.setDataSource(dataSrc);

            /* Create zone in the storage profile */
            client.sql().execute(null,
                    "CREATE ZONE MYZONE WITH STORAGE_PROFILES='default'");

            /* Create cache */
            client.sql().execute(null,
                    "CREATE CACHE Accounts ("
                            + "accountNumber INT PRIMARY KEY,"
                            + "firstName VARCHAR,"
                            + "lastName VARCHAR,"
                            + "balance DOUBLE"
                            + ") ZONE MYZONE"
            );

            /* Get key-value view from cache */
            var kvView = client.caches()
                    .cache("Accounts")
                    .keyValueView(storeFactory,
                            Mapper.of(AccountKey.class),
                            Mapper.of(Account.class));

            /* Update cache */
            updateCache(kvView);

            /* Run cache-only transaction */
            runCacheOnlyTx(client, kvView);
        }
    }

    private static void updateCache(KeyValueView kvView) {
        AccountKey k = new AccountKey(1);
        Account a = new Account("Alice", "Doe", 500);
        kvView.put(null, k, a);
        System.out.println("Read entry from Cache: " + kvView.get(null, k));
    }

    private static void runCacheOnlyTx(IgniteClient client, KeyValueView kvView) {
        AccountKey k2 = new AccountKey(2);
        Account a2 = new Account("Bob", "Smith", 750);

        Transaction tx = client.transactions()
                .begin(new TransactionOptions().cacheOnly(true).timeoutMillis(5));

        boolean committed = false;

        try {
            kvView.put(tx, k2, a2);
            committed = true;
        } finally {
            if (!committed) {
                tx.rollback();
            }

            /* Read value that was updated via transaction */
            System.out.println("Bob:   " + kvView.get(null, k2));
        }
    }

    private static JdbcType getJdbcTypeMapping() {
        var jdbcType = new JdbcType();
        jdbcType.setDatabaseSchema("PUBLIC");
        jdbcType.setDatabaseTable("Accounts");
        jdbcType.setKeyType(AccountKey.class);
        jdbcType.setKeyFields(
                new JdbcTypeField(Types.INTEGER, "accountNumber",
                        Integer.class, "accountNumber"));
        jdbcType.setValueType(Account.class);
        jdbcType.setValueFields(
                new JdbcTypeField(Types.VARCHAR, "firstName", String.class, "firstName"),
                new JdbcTypeField(Types.VARCHAR, "lastName", String.class, "lastName"),
                new JdbcTypeField(Types.DOUBLE, "balance", Double.class, "balance"));
        return jdbcType;
    }
}
