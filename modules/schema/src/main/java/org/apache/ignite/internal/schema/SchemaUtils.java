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

import java.util.List;
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
        List<Column> oldCols = oldDesc.columns();
        List<Column> newCols = newDesc.columns();

        ColumnMapper mapper = null;

        for (int i = 0; i < newCols.size(); ++i) {
            Column newCol = newCols.get(i);

            if (i < oldCols.size()) {
                Column oldCol = oldCols.get(i);

                if (newCol.positionInRow() == oldCol.positionInRow()) {
                    if (!newCol.name().equals(oldCol.name())) {
                        if (mapper == null) {
                            mapper = ColumnMapping.createMapper(newDesc);
                        }

                        Column oldIdx = oldDesc.column(newCol.name());

                        // rename
                        if (oldIdx != null) {
                            mapper.add(newCol.positionInRow(), oldIdx.positionInRow());
                        }
                    }
                } else {
                    if (mapper == null) {
                        mapper = ColumnMapping.createMapper(newDesc);
                    }

                    if (newCol.name().equals(oldCol.name())) {
                        mapper.add(newCol.positionInRow(), oldCol.positionInRow());
                    } else {
                        Column oldIdx = oldDesc.column(newCol.name());

                        assert oldIdx != null : newCol.name();

                        mapper.add(newCol.positionInRow(), oldIdx.positionInRow());
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
        if (exp.columns().size() != actual.columns().size()) {
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
