package org.apache.ignite.jdbc.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Functional interface for extracting a value from the current row of a {@link ResultSet}.
 *
 * @param <T> Type of the extracted value.
 */
@FunctionalInterface
public interface RowColumnProjection<T> {
    /** Extracts a value from the current row of {@code rs}. */
    T extract(ResultSet rs) throws SQLException;

    /** Drains result set to list by projecting each record with provided extractor */
    static <T> List<T> projectRowsColumn(ResultSet rs, RowColumnProjection<T> extractor) throws SQLException {
        List<T> result = new ArrayList<>();

        while (rs.next()) {
            result.add(extractor.extract(rs));
        }

        return result;
    }
}
