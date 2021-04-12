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

package org.apache.ignite.internal.table.schema;

import java.util.List;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableSchemaManager;

/**
 * Table schema manager component.
 */
public class TableSchemaManagerImpl implements TableSchemaManager {
    /** Stub. */
    private static final Column[] EMPTY_COLS_ARR = new Column[0];

    /** Local registry for schema. */
    private final SchemaRegistry schemaReg;

    /**
     * Constructor.
     */
    public TableSchemaManagerImpl() {
        this.schemaReg = new SchemaRegistry();
    }

    /**
     * Gets schema description for version.
     *
     * @param ver Schema version to get descriptor for.
     * @return Schema descriptor.
     */
    @Override public SchemaDescriptor schema(int ver) {
        return schemaReg.schema(ver);
    }

    /**
     * Gets schema description for version.
     *
     * @return Schema descriptor.
     */
    @Override public SchemaDescriptor schema() {
        return schemaReg.schema();
    }

    /**
     * Registers schema in local registry.
     *
     * @param desc Schema descriptor.
     */
    private void registerSchemaLocal(SchemaDescriptor desc) {
        schemaReg.registerSchema(desc);
    }

    /**
     * Metastore callback is triggered when new schema was registered in grid.
     *
     * @param schemaDesc Schema descriptor.
     */
    public void onSchemaUpdated(SchemaDescriptor schemaDesc) {
        registerSchemaLocal(schemaDesc);
    }

    /**
     * Compares schemas.
     *
     * @param expected Expected schema.
     * @param actual Actual schema.
     * @return {@code True} if schemas are equal, {@code false} otherwise.
     */
    static boolean equalSchemas(SchemaDescriptor expected, SchemaDescriptor actual) {
        if (expected.keyColumns().length() != actual.keyColumns().length() ||
            expected.valueColumns().length() != actual.valueColumns().length())
            return false;

        for (int i = 0; i < expected.length(); i++) {
            if (!expected.column(i).equals(actual.column(i)))
                return false;
        }

        return true;
    }

    /**
     * Schema descriptor factory method.
     *
     * @param ver Version.
     * @param keyCols Key columns.
     * @param valCols Value columns.
     * @return Schema descriptor.
     */
    static SchemaDescriptor createDescriptor(int ver, List<Column> keyCols, List<Column> valCols) {
        return new SchemaDescriptor(
            ver,
            keyCols.toArray(EMPTY_COLS_ARR),
            valCols.toArray(EMPTY_COLS_ARR));
    }
}
