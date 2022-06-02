package org.apache.ignite.cli.sql.table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Data class for table representation.
 *
 * @param <T> type of table elements.
 */
public class Table<T> {
    private final String[] header;
    private final List<TableRow<T>> content;

    /**
     * Constructor.
     *
     * @param ids list of column names.
     * @param content list of row content. Size should be equals n * ids.size.
     */
    public Table(List<String> ids, List<T> content) {
        if (content.size() != 0 && ids.size() != 0 && content.size() % ids.size() != 0) {
            throw new IllegalArgumentException("Content size should be divisible by columns count");
        }

        this.header = parseHeader(ids);
        this.content = new ArrayList<>();
        int columnsCount = ids.size();
        int n = columnsCount != 0 ? content.size() / columnsCount : 0;
        for (int i = 0; i < n; i++) {
            List<T> elements = content.subList(i * columnsCount, (i + 1) * columnsCount);
            this.content.add(new TableRow<>(elements));
        }
    }

    private static String[] parseHeader(List<String> header) {
        if (header.size() > 0) {
            return header.toArray(new String[0]);
        } else {
            return new String[] { "EMPTY" };
        }
    }

    /**
     * Table header getter.
     *
     * @return array of table's columns name.
     */
    public String[] header() {
        return header;
    }

    /**
     * Table content getter.
     *
     * @return content of table without header.
     */
    public Object[][] content() {
        List<Object[]> collect = content.stream()
                .map(row -> new ArrayList<>(row.getValues()))
                .map(strings -> strings.toArray(new Object[0]))
                .collect(Collectors.toList());

        return collect.toArray(new Object[0][0]);
    }

    /**
     * Create method.
     *
     * @param resultSet coming result set.
     * @return istance of {@link Table}.
     */
    public static Table<String> fromResultSet(ResultSet resultSet) {
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> ids = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                ids.add(metaData.getColumnName(i));
            }
            List<String> content = new ArrayList<>();
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    content.add(resultSet.getString(i));
                }
            }
            return new Table<>(ids, content);
        } catch (SQLException e) {
            return null;
        }
    }
}
