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

package org.apache.ignite.internal.cli.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * Loader of {@link SqlSchema}.
 */
public class SqlSchemaLoader {
    private static final IgniteLogger LOG = CliLoggers.forClass(SqlSchemaLoader.class);

    private final MetadataSupplier metadataSupplier;

    public SqlSchemaLoader(MetadataSupplier metadataSupplier) {
        this.metadataSupplier = metadataSupplier;
    }

    /**
     * Load and return {@link SqlSchema}.
     *
     * @return instance of {@link SqlSchema}.
     */
    public SqlSchema loadSchema() {
        Map<String, Map<String, Set<String>>> schema = new HashMap<>();
        try (ResultSet rs = metadataSupplier.getMetaData().getTables(null, null, null, null)) {
            while (rs.next()) {
                String tableSchema = rs.getString("TABLE_SCHEM");
                Map<String, Set<String>> tables = schema.computeIfAbsent(tableSchema, schemaName -> new HashMap<>());

                String tableName = rs.getString("TABLE_NAME");
                tables.put(tableName, loadColumns(tableSchema, tableName));
            }
        } catch (SQLException e) {
            LOG.error("Error during sql schema load", e);
        }
        return new SqlSchema(schema);
    }


    private Set<String> loadColumns(String schemaName, String tableName) {
        Set<String> columns = new HashSet<>();
        try (ResultSet rs = metadataSupplier.getMetaData().getColumns(null, schemaName, tableName, null)) {

            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            LOG.error("Error during sql schema load", e);
        }
        return columns;
    }
}
