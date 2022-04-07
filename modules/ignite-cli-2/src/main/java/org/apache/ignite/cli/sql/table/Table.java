package org.apache.ignite.cli.sql.table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.cli.commands.decorators.core.CommandOutput;

public class Table<T> implements Iterable<T>, CommandOutput {
    private final Map<String, TableRow<T>> content;
    
    public Table(List<String> ids, List<T> content) {
        if (content.size() % ids.size() != 0) {
            throw new RuntimeException();
        }
        
        this.content = new HashMap<>();
        for (int i = 0, size = ids.size(); i < size; i++) {
            String id = ids.get(i);
            TableRow<T> row = new TableRow<>(id, content.subList(i * size, (i + 1) * size));
            this.content.put(id, row);
        }
    }
    
    
    public TableRow<T> getRow(String id) {
        return content.get(id);
    }
    
    @Override
    public Iterator<T> iterator() {
        return content.values().stream()
                .flatMap((Function<TableRow<T>, Stream<T>>) ts -> StreamSupport.stream(ts.spliterator(), false))
                .collect(Collectors.toList())
                .iterator();
    }
    
    @Override
    public String get() {
        return StreamSupport.stream(spliterator(), false).map(T::toString).collect(Collectors.joining(", "));
    }
    
    
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
