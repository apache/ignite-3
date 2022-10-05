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

import static org.apache.ignite.internal.schema.SchemaManager.INITIAL_SCHEMA_VERSION;
import static org.apache.ignite.internal.schema.SchemaManager.SCHEMA_STORE_PREDICATE;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.schema.configuration.ConfigurationToSchemaDescriptorConverter;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Stateless schema utils that produces helper methods for schema preparation.
 */
public class SchemaUtils {
    /**
     * Creates schema descriptor for the table with specified configuration.
     *
     * @param schemaVer Schema version.
     * @param tableView Table configuration.
     * @return Schema descriptor.
     */
    public static SchemaDescriptor prepareSchemaDescriptor(int schemaVer, TableView tableView) {
        return ConfigurationToSchemaDescriptorConverter.convert(schemaVer, tableView);
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
     * Forms schema history key.
     *
     * @param tblId Table id.
     * @param ver Schema version.
     * @return {@link ByteArray} representation.
     */
    public static ByteArray schemaWithVerHistKey(UUID tblId, int ver) {
        return ByteArray.fromString(tblId + SCHEMA_STORE_PREDICATE + ver);
    }

    /**
     * Forms schema history predicate.
     *
     * @param tblId Table id.
     * @return {@link ByteArray} representation.
     */
    public static ByteArray schemaHistPredicate(UUID tblId) {
        return ByteArray.fromString(tblId + SCHEMA_STORE_PREDICATE);
    }

    /**
     * Gets the latest version of the table schema which available in Metastore.
     *
     * @param tblId Table id.
     * @param metastorageMgr Metastorage manager.
     * @return The latest schema version.
     */
    public static int latestSchemaVersion(MetaStorageManager metastorageMgr, UUID tblId) {
        try {
            Cursor<Entry> cur = metastorageMgr.prefix(schemaHistPredicate(tblId));

            int lastVer = INITIAL_SCHEMA_VERSION;

            for (Entry ent : cur) {
                String key = ent.key().toString();
                int pos = key.indexOf(':');
                assert pos != -1 : "Unexpected key: " + key;

                key = key.substring(pos + 1);
                int descVer = Integer.parseInt(key);

                if (descVer > lastVer) {
                    lastVer = descVer;
                }
            }

            return lastVer;
        } catch (NoSuchElementException e) {
            assert false : "Table must exist. [tableId=" + tblId + ']';

            return INITIAL_SCHEMA_VERSION;
        } catch (NodeStoppingException e) {
            throw new IgniteException(e.traceId(), e.code(), e.getMessage(), e);
        }
    }

    /**
     * Gets the latest serialized schema of the table which available in Metastore.
     *
     * @param tblId Table id.
     * @param metastorageMgr Metastorage manager.
     * @return The latest schema version or {@code null} if not found.
     */
    public static @Nullable byte[] schemaById(MetaStorageManager metastorageMgr, UUID tblId, int ver) {
        try {
            Cursor<Entry> cur = metastorageMgr.prefix(schemaHistPredicate(tblId));

            int lastVer = INITIAL_SCHEMA_VERSION;
            byte[] schema = null;

            for (Entry ent : cur) {
                String key = ent.key().toString();
                int pos = key.indexOf(':');
                assert pos != -1 : "Unexpected key: " + key;

                key = key.substring(pos + 1);
                int descVer = Integer.parseInt(key);

                if (ver != -1) {
                    if (ver == descVer) {
                        return ent.value();
                    }
                } else if (descVer >= lastVer) {
                    lastVer = descVer;
                    schema = ent.value();
                }
            }

            return schema;
        } catch (NoSuchElementException e) {
            assert false : "Table must exist. [tableId=" + tblId + ']';

            return null;
        } catch (NodeStoppingException e) {
            throw new IgniteException(e.traceId(), e.code(), e.getMessage(), e);
        }
    }
}
