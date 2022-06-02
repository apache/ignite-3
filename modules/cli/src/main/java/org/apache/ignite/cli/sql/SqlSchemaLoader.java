package org.apache.ignite.cli.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Loader of {@link SqlSchema}.
 */
public class SqlSchemaLoader {
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
            //todo report error
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
            //todo report error
        }
        return columns;
    }
}
