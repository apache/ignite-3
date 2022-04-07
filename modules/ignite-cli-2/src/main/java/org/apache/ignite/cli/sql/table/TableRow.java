package org.apache.ignite.cli.sql.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TableRow<T> implements Iterable<T> {
    public final String id;
    private final List<T> content;
    
    public TableRow(String id, Collection<T> elements) {
        this.id = id;
        content = new ArrayList<>(elements);
    }
    
    public T get(int index) {
        return content.get(index);
    }
    
    public Collection<T> getValues() {
        return Collections.unmodifiableCollection(content);
    }
    
    @Override
    public Iterator<T> iterator() {
        return content.iterator();
    }
}
