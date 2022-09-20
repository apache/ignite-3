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

import java.util.Optional;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.configuration.SchemaDescriptorConverter;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.schema.definition.TableDefinition;

/**
 * Stateless schema utils that produces helper methods for schema preparation.
 */
public class SchemaUtils {
    /**
     * Creates schema descriptor for the table with specified configuration.
     *
     * @param schemaVer Schema version.
     * @param tblCfg    Table configuration.
     * @return Schema descriptor.
     */
    public static SchemaDescriptor prepareSchemaDescriptor(int schemaVer, TableView tblCfg) {
        TableDefinition tableDef = SchemaConfigurationConverter.convert(tblCfg);

        return SchemaDescriptorConverter.convert(schemaVer, tableDef);
    }

    /**
     * Prepares column mapper.
     *
     * @param oldDesc Old schema descriptor.
     * @param oldTblColumns Old columns configuration.
     * @param newDesc New schema descriptor.
     * @param newTblColumns New columns configuration.
     * @return Column mapper.
     */
    public static ColumnMapper columnMapper(
            SchemaDescriptor oldDesc,
            NamedListView<? extends ColumnView> oldTblColumns,
            SchemaDescriptor newDesc,
            NamedListView<? extends ColumnView> newTblColumns
    ) {
        ColumnMapper mapper = null;

        // since newTblColumns comes from a Change class, it can only be of the same size or larger than the previous configuration,
        // because removed keys are simply replaced with nulls
        assert newTblColumns.size() >= oldTblColumns.size();

        for (int i = 0; i < newTblColumns.size(); ++i) {
            ColumnView newColView = newTblColumns.get(i);

            // new value can be null if a column has been deleted
            if (newColView == null) {
                continue;
            }

            if (i < oldTblColumns.size()) {
                ColumnView oldColView = oldTblColumns.get(i);

                Column newCol = newDesc.column(newColView.name());
                Column oldCol = oldDesc.column(oldColView.name());

                if (newCol.schemaIndex() == oldCol.schemaIndex()) {
                    continue;
                }

                if (mapper == null) {
                    mapper = ColumnMapping.createMapper(newDesc);
                }

                mapper.add(newCol.schemaIndex(), oldCol.schemaIndex());
            } else {
                // if the new Named List is larger than the old one, it can only mean that a new column has been added
                Column newCol = newDesc.column(newColView.name());

                assert !newDesc.isKeyColumn(newCol.schemaIndex());

                if (mapper == null) {
                    mapper = ColumnMapping.createMapper(newDesc);
                }

                mapper.add(newCol);
            }
        }

        // since newTblColumns comes from a TableChange, it will contain nulls for removed columns
        Optional<Column> droppedKeyCol = newTblColumns.namedListKeys().stream()
                .filter(k -> newTblColumns.get(k) == null)
                .map(oldDesc::column)
                .filter(c -> oldDesc.isKeyColumn(c.schemaIndex()))
                .findAny();

        // TODO: IGNITE-15774 Assertion just in case, proper validation should be implemented with the help of
        // TODO: configuration validators.
        assert droppedKeyCol.isEmpty() :
                IgniteStringFormatter.format(
                        "Dropping of key column is forbidden: [schemaVer={}, col={}]",
                        newDesc.version(),
                        droppedKeyCol.get()
                );

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

    /**
     * Creates canonical table name.
     *
     * @return Table with schema canonical name.
     */
    public static String canonicalName(String schema, String name) {
        return schema + '.' + name;
    }

    /**
     * Extracts schema from canonical, made by {@link #canonicalName(String, String)}.
     *
     * @param canonicalName Canonical name.
     * @return Schema.
     */
    public static String extractSchema(String canonicalName) {
        int sepPos = canonicalName.indexOf('.');
        assert sepPos != -1 : "No schema defined in " + canonicalName;
        return canonicalName.substring(0, sepPos);
    }
}
