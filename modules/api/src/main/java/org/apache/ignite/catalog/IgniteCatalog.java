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

import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;

/**
 * Provides the ability to create and execute SQL DDL queries from annotated classes. This is an example of the simple table:
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
 * This is an example of the table created from simple annotated record class.
 * <pre>
 *    &#064;Table
 *    private static class Pojo {
 *        &#064;Id
 *        Integer id;
 *    }
 * </pre>
 * When this class is passed to the {@link #create(Class)} method it will produce the following statement:
 * <pre>
 *    CREATE TABLE IF NOT EXISTS Pojo (id int, PRIMARY KEY (id));
 * </pre>
 * Here's an example of more complex annotations including zones, colocation and indexes:
 * <pre>
 *    &#064;@Zone(
 *            value = "zone_test",
 *            replicas = 3,
 *            partitions = 1,
 *            engine = ZoneEngine.AIMEM
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
 * These classes when passed to the {@link #create(Class)} method will produce the following statements:
 * <pre>
 *    CREATE ZONE IF NOT EXISTS zone_test ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;
 *    CREATE TABLE IF NOT EXISTS table_test (id int, id_str varchar(20), f_name varchar(20) not null default 'a', \
 *    l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';
 *    CREATE INDEX IF NOT EXISTS ix_pojo ON table_test (f_name, l_name desc);
 * </pre>
 */
public interface IgniteCatalog {
    /**
     * Creates a query object from the annotated record class.
     *
     * @param recordClass Annotated record class.
     * @return query object
     */
    Query create(Class<?> recordClass);

    /**
     * Creates a query object from the annotated key and value classes.
     *
     * @param keyClass Annotated key class.
     * @param valueClass Annotated value class.
     * @return query object
     */
    Query create(Class<?> keyClass, Class<?> valueClass);

    /**
     * Creates a query object from the table definition.
     *
     * @param definition Table definition.
     * @return query object
     */
    Query createTable(TableDefinition definition);

    /**
     * Creates a query object from the zone definition.
     *
     * @param definition Zone definition.
     * @return query object
     */
    Query createZone(ZoneDefinition definition);

    /**
     * Creates a {@code DROP TABLE} query object from the table definition.
     *
     * @param definition Table definition.
     * @return query object
     */
    Query dropTable(TableDefinition definition);

    /**
     * Creates a {@code DROP TABLE} query object from the table name.
     *
     * @param name Table name.
     * @return query object
     */
    Query dropTable(String name);

    /**
     * Creates a {@code DROP ZONE} query object from the zone definition.
     *
     * @param definition Zone definition.
     * @return query object
     */
    Query dropZone(ZoneDefinition definition);

    /**
     * Creates a {@code DROP ZONE} query object from the zone name.
     *
     * @param name Zone name.
     * @return query object
     */
    Query dropZone(String name);
}
