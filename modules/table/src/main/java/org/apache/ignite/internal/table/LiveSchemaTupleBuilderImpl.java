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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
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
    private Map<String, Object> liveSchemaColMap;

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

        liveSchemaColMap = null;
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder set(String colName, Object val) {
        Column col = schema().column(colName);

        if (col == null) {
            if (val == null)
                return this;

            if (liveSchemaColMap == null)
                liveSchemaColMap = new HashMap<>();
                
            liveSchemaColMap.put(colName, val);
            
            return this;
        }
        super.set(colName, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Tuple build() {
        if (liveSchemaColMap == null)
            return this;

        while (!liveSchemaColMap.isEmpty()) {
            createColumns(liveSchemaColMap);
        
            this.schema(schemaRegistry.schema());
        
            Map colMap = map;
            map = new HashMap();
            liveSchemaColMap.clear();

            colMap.forEach(super::set);
        }

        return this;
    }

    /**
     * Updates the schema, creates new columns.
     * @param colTypeMap - map with column names and column types.
     */
    private void createColumns(Map<String, ColumnType> colTypeMap) {
        List<org.apache.ignite.schema.Column> newCols = colTypeMap.entrySet().stream()
            .map(entry -> SchemaBuilders.column(entry.getKey(), entry.getValue()).asNullable().build())
            .collect(Collectors.toList());

        mgr.alterTable(tblName, chng -> chng.changeColumns(cols -> {
            int colIdx = chng.columns().size();
            //TODO: avoid 'colIdx' or replace with correct last colIdx.

            for (org.apache.ignite.schema.Column column : newCols) {
                cols.create(String.valueOf(colIdx), colChg -> convert(column, colChg));
                colIdx++;
            }
        }));
    }

    /**
     * Validate all column values after updating schema.
     */
    private void rebuildTupleWithNewSchema() {
        Collection<String> colNames = schema().columnNames();

        colNames.stream().filter(map::containsKey).forEach(name -> set(name, map.get(name)));
    }
}
