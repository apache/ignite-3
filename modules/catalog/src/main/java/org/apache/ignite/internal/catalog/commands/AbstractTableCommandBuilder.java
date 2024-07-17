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

package org.apache.ignite.internal.catalog.commands;

import org.apache.ignite.internal.catalog.CatalogCommand;

/**
 * Abstract builder of table-related commands.
 *
 * <p>Every table-related command, disregard it going to create new table or modify existing one,
 * should specify name of the table and namespace (schema) where to find existing/put new table.
 */
public interface AbstractTableCommandBuilder<T extends AbstractTableCommandBuilder<T>> {
    /** A name of the schema a table belongs to. Should not be null or blank. */
    T schemaName(String schemaName);

    /** A name of the table. Should not be null or blank. */
    T tableName(String tableName);

    /** Sets a flag indicating whether the {@code IF EXISTS} was specified. */
    T ifTableExists(boolean ifTableExists);

    /** Returns a command with specified parameters. */
    CatalogCommand build();
}
