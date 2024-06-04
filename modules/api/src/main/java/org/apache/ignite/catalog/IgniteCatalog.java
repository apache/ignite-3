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

package org.apache.ignite.catalog;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.table.Table;

/**
 * Provides the ability to create and execute SQL DDL queries from annotated classes or fluent builders. This is an example of the simple
 * table created from the annotated class:
 * <pre>
 *    &#064;Table
 *    private static class Value {
 *        String val;
 *    }
 *    ignite.catalog().create(Integer.class, Value.class).execute();
 * </pre>
 * This is equivalent to executing the following statement:
 * <pre>
 *     CREATE TABLE IF NOT EXISTS Value (id int, val varchar, PRIMARY KEY (id));
 * </pre>
 * The same statement can be produced using builders:
 * <pre>
 *    TableDefinition table = TableDefinition.builder("Value")
 *            .ifNotExists()
 *            .columns(
 *                    column("id", INTEGER),
 *                    column("val", VARCHAR)
 *            )
 *            .primaryKey("id")
 *            .build();
 *    ignite.catalog().createTable(table).execute();
 * </pre>
 * This is an example of the table created from simple annotated record class.
 * <pre>
 *    &#064;Table
 *    private static class Pojo {
 *        &#064;Id
 *        Integer id;
 *    }
 * </pre>
 * When this class is passed to the {@link #createTableAsync(Class)} method it will produce the following statement:
 * <pre>
 *    CREATE TABLE IF NOT EXISTS Pojo (id int, PRIMARY KEY (id));
 * </pre>
 * Again, using builders this can be written as:
 * <pre>
 *    TableDefinition table = TableDefinition.builder("Pojo")
 *            .ifNotExists()
 *            .columns(column("id", INTEGER))
 *            .primaryKey("id")
 *            .build();
 *    ignite.catalog().createTable(table).execute();
 * </pre>
 * Here's an example of more complex annotations including zones, colocation and indexes:
 * <pre>
 *    &#064;@Zone(
 *            value = "zone_test",
 *            replicas = 3,
 *            partitions = 1,
 *            storageProfiles = "default"
 *    )
 *    private static class ZoneTest {}
 *
 *    &#064;Table(
 *            value = "table_test",
 *            zone = ZoneTest.class,
 *            colocateBy = &#064;ColumnRef("id"),
 *            indexes = &#064;Index(value = "ix_pojo", columns = {
 *                    &#064;ColumnRef("f_name"),
 *                    &#064;ColumnRef(value = "l_name", sort = SortOrder.DESC)
 *            })
 *    )
 *    private static class Pojo {
 *        &#064;Id
 *        Integer id;
 *
 *        &#064;Id
 *        &#064;Column(value = "id_str", length = 20)
 *        String idStr;
 *
 *        &#064;Column(value = "f_name", columnDefinition = "varchar(20) not null default 'a'")
 *        String firstName;
 *
 *        &#064;Column("l_name")
 *        String lastName;
 *
 *        String str;
 *    }
 * </pre>
 * These classes when passed to the {@link #createTableAsync(Class)} method will produce the following statements:
 * <pre>
 *    CREATE ZONE IF NOT EXISTS zone_test WITH PARTITIONS=1, REPLICAS=3, STORAGE_PROFILES='default';
 *    CREATE TABLE IF NOT EXISTS table_test (id int, id_str varchar(20), f_name varchar(20) not null default 'a', \
 *    l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';
 *    CREATE INDEX IF NOT EXISTS ix_pojo ON table_test (f_name, l_name desc);
 * </pre>
 * And here's the equivalent definition using builders:
 * <pre>
 *    ZoneDefinition zone = ZoneDefinition.builder("zone_test")
 *             .ifNotExists()
 *             .partitions(1)
 *             .replicas(3)
 *             .storageProfiles("default")
 *             .build();
 *    TableDefinition table = TableDefinition.builder("table_test")
 *            .ifNotExists()
 *            .columns(
 *                    column("id", ColumnType.INTEGER),
 *                    column("id_str", ColumnType.varchar(20)),
 *                    column("f_name", ColumnType.varchar(20).notNull().defaultValue("a")),
 *                    column("l_name", ColumnType.VARCHAR),
 *                    column("str", ColumnType.VARCHAR),
 *            )
 *            .primaryKey("id", "id_str")
 *            .colocateBy("id", "id_str")
 *            .zone("zone_test")
 *            .index("ix_pojo", IndexType.DEFAULT, column("f_name"), column("l_name").desc())
 *            .build();
 *    ignite.catalog().createZone(zone).execute();
 *    ignite.catalog().createTable(table).execute();
 * </pre>
 *
 *
 */
public interface IgniteCatalog {
    /**
     * Creates a query object from the annotated record class.
     *
     * @param recordClass Annotated record class.
     * @return Query object.
     */
    CompletableFuture<Table> createTableAsync(Class<?> recordClass);

    /**
     * Creates a query object from the annotated key and value classes.
     *
     * @param keyClass Annotated key class.
     * @param valueClass Annotated value class.
     * @return Query object.
     */
    CompletableFuture<Table> createTableAsync(Class<?> keyClass, Class<?> valueClass);

    /**
     * Creates a query object from the table definition.
     *
     * @param definition Table definition.
     * @return Query object.
     */
    CompletableFuture<Table> createTableAsync(TableDefinition definition);

    /**
     * Creates a query object from the zone definition.
     *
     * @param definition Zone definition.
     */
    CompletableFuture<Void> createZoneAsync(ZoneDefinition definition);

    /**
     * Creates a {@code DROP TABLE} query object from the table definition.
     *
     * @param definition Table definition.
     */
    CompletableFuture<Void> dropTableAsync(TableDefinition definition);

    /**
     * Creates a {@code DROP TABLE} query object from the table name.
     *
     * @param name Table name.
     */
    CompletableFuture<Void> dropTableAsync(String name);

    /**
     * Creates a {@code DROP ZONE} query object from the zone definition.
     *
     * @param definition Zone definition.
     */
    CompletableFuture<Void> dropZoneAsync(ZoneDefinition definition);

    /**
     * Creates a {@code DROP ZONE} query object from the zone name.
     *
     * @param name Zone name.
     */
    CompletableFuture<Void> dropZoneAsync(String name);
}
