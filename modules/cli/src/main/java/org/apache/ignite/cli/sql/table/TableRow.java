package org.apache.ignite.cli.sql.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Data class for table row representation.
 *
 * @param <T> type of table row elements.
 */
public class TableRow<T> implements Iterable<T> {
    private final List<T> content;

    /**
     * Constructor.
     *
     * @param elements row elements.
     */
    public TableRow(Collection<T> elements) {
        content = new ArrayList<>(elements);
    }

    /**
     * Element getter.
     *
     * @param index in table row.
     * @return Element of table row with index {@param index}.
     */
    public T get(int index) {
        return content.get(index);
    }

    /**
     * Getter for table row elements.
     *
     * @return Unmodifiable collection of table row content.
     */
    public Collection<T> getValues() {
        return Collections.unmodifiableCollection(content);
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<T> iterator() {
        return content.iterator();
    }
}
