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

package org.apache.ignite.internal.schema;

import java.util.Arrays;
import java.util.Comparator;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.catalog.CatalogToSchemaDescriptorConverter;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;

/**
 * Stateless schema utils that produces helper methods for schema preparation.
 */
public class SchemaUtils {
    /**
     * Creates schema descriptor for the table with specified descriptor.
     *
     * @param tableDescriptor Table descriptor.
     * @return Schema descriptor.
     */
    public static SchemaDescriptor prepareSchemaDescriptor(CatalogTableDescriptor tableDescriptor) {
        return CatalogToSchemaDescriptorConverter.convert(tableDescriptor, tableDescriptor.tableVersion());
    }

    /**
     * Prepares column mapper.
     *
     * @param oldDesc Old schema descriptor.
     * @param newDesc New schema descriptor.
     * @return Column mapper.
     */
    public static ColumnMapper columnMapper(
            SchemaDescriptor oldDesc,
            SchemaDescriptor newDesc
    ) {
        Column[] cols = oldDesc.valueColumns().columns();
        Column[] oldCols = Arrays.copyOf(cols, cols.length);

        Arrays.sort(oldCols, Comparator.comparingInt(Column::columnOrder));

        cols = newDesc.valueColumns().columns();
        Column[] newCols = Arrays.copyOf(cols, cols.length);

        Arrays.sort(newCols, Comparator.comparingInt(Column::columnOrder));

        ColumnMapper mapper = null;

        for (int i = 0; i < newCols.length; ++i) {
            Column newCol = newCols[i];

            if (i < oldCols.length) {
                Column oldCol = oldCols[i];

                if (newCol.schemaIndex() == oldCol.schemaIndex()) {
                    if (!newCol.name().equals(oldCol.name())) {
                        if (mapper == null) {
                            mapper = ColumnMapping.createMapper(newDesc);
                        }

                        Column oldIdx = oldDesc.column(newCol.name());

                        // rename
                        if (oldIdx != null) {
                            mapper.add(newCol.schemaIndex(), oldIdx.schemaIndex());
                        }
                    }
                } else {
                    if (mapper == null) {
                        mapper = ColumnMapping.createMapper(newDesc);
                    }

                    if (newCol.name().equals(oldCol.name())) {
                        mapper.add(newCol.schemaIndex(), oldCol.schemaIndex());
                    } else {
                        Column oldIdx = oldDesc.column(newCol.name());

                        assert oldIdx != null : newCol.name();

                        mapper.add(newCol.schemaIndex(), oldIdx.schemaIndex());
                    }
                }
            } else {
                if (mapper == null) {
                    mapper = ColumnMapping.createMapper(newDesc);
                }

                mapper.add(newCol);
            }
        }

        return mapper == null ? ColumnMapping.identityMapping() : mapper;
    }

    /**
     * Compares schemas.
     *
     * @param exp    Expected schema.
     * @param actual Actual schema.
     * @return {@code True} if schemas are equal, {@code false} otherwise.
     */
    public static boolean equalSchemas(SchemaDescriptor exp, SchemaDescriptor actual) {
        if (exp.keyColumns().length() != actual.keyColumns().length()
                || exp.valueColumns().length() != actual.valueColumns().length()) {
            return false;
        }

        for (int i = 0; i < exp.length(); i++) {
            if (!exp.column(i).equals(actual.column(i))) {
                return false;
            }
        }

        return true;
    }
}
