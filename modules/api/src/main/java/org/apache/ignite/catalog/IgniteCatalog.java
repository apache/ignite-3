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
 * Provides the ability to create and execute SQL DDL queries from annotated classes or from fluent-style builders.
 */
public interface IgniteCatalog {
    /**
     * Creates a query object from the annotated record class.
     *
     * @param recCls Annotated record class.
     * @return query object
     */
    Query create(Class<?> recCls);

    /**
     * Creates a query object from the annotated key and value classes.
     *
     * @param key Annotated key class.
     * @param value Annotated value class.
     * @return query object
     */
    Query create(Class<?> key, Class<?> value);

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
