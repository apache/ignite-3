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

import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;

/**
 * Abstract table-related command.
 *
 * <p>Every table-related command, disregard it going to create new table or modify existing one,
 * should specify name of the table and namespace (schema) where to find existing/put new table.
 */
public abstract class AbstractTableCommand implements CatalogCommand {
    protected final String schemaName;

    protected final String tableName;

    protected final boolean ifTableExists;

    AbstractTableCommand(String schemaName, String tableName, boolean ifTableExists) throws CatalogValidationException {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.ifTableExists = ifTableExists;

        validate();
    }

    public boolean ifTableExists() {
        return ifTableExists;
    }

    private void validate() {
        if (schemaName != null && CatalogUtils.isSystemSchema(schemaName)) {
            throw new CatalogValidationException(format("Operations with reserved schemas are not allowed, schema: {}", schemaName));
        }
        validateIdentifier(schemaName, "Name of the schema");
        validateIdentifier(tableName, "Name of the table");
    }
}
