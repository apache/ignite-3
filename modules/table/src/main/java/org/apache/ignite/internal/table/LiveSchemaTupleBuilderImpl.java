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

package org.apache.ignite.internal.table;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;

/**
 * Live schema buildable tuple.
 *
 * Allows to create columns implicitly by adding previously nonexistent columns during insert.
 */
public class LiveSchemaTupleBuilderImpl extends TupleBuilderImpl {
    /** Live schema column values. */
    private final Map<String, Object> liveSchemaColMap;

    /** Schema registry. */
    private final SchemaRegistry schemaRegistry;

    /** Current table name. */
    private final String tblName;

    /** Table manager. */
    private final TableManager mgr;

    /**
     * Constructor.
     */
    public LiveSchemaTupleBuilderImpl(SchemaRegistry schemaRegistry, String tblName, TableManager mgr) {
        super(schemaRegistry == null ? null : schemaRegistry.schema());

        Objects.requireNonNull(schemaRegistry);
        Objects.requireNonNull(tblName);
        Objects.requireNonNull(mgr);

        this.schemaRegistry = schemaRegistry;
        this.tblName = tblName;
        this.mgr = mgr;

        liveSchemaColMap = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder set(String colName, Object val) {
        Column col = schemaRegistry.schema().column(colName);

        if (col == null) {
            if (val == null)
                return this;

            liveSchemaColMap.put(colName, val);
            return this;
        }

        super.set(colName, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Tuple build() {
        for (Map.Entry<String, Object> entry : liveSchemaColMap.entrySet()) {
            String colName = entry.getKey();
            Object val = entry.getValue();

            ColumnType type = MarshallerUtil.columnType(val.getClass());

            if (type == null)
                throw new UnsupportedOperationException("Live schema update for type [" + val.getClass() + "] is not supported yet.");

            createColumn(colName, type);

            super.set(colName, val);
        }
        return this;
    }

    /**
     * @param colName
     * @param type
     */
    private void createColumn(String colName, ColumnType type) {
        org.apache.ignite.schema.Column schemaCol = SchemaBuilders.column(colName, type).asNullable().build();

        mgr.alterTable(tblName, chng -> chng.changeColumns(cols -> {
            final int colIdx = chng.columns().size();
            //TODO: avoid 'colIdx' or replace with correct last colIdx.
            cols.create(String.valueOf(colIdx), colChg -> convert(schemaCol, colChg));
        }));
    }

    /**
     * Get last schema descriptor.
     *
     * @return Schema descriptor.
     */
    public SchemaDescriptor schema() {
        return schemaRegistry.schema();
    }
}
