package org.apache.ignite.cli.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Supplier for SQL metadata.
 */
public interface MetadataSupplier {
    /**
     * Retrieves database metadata as in {@link Connection#getMetaData()}.
     *
     * @return a metadata object
     * @throws SQLException if a database access error occurs
     */
    DatabaseMetaData getMetaData() throws SQLException;
}
