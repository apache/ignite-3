package org.apache.ignite.schema;

import java.util.Collection;

public interface SortedIndex extends TableIndex {
    /**
     * @return Index inline size.
     */
    int inlineSize();

    /**
     * Return user defined indexed columns.
     *
     * @return Declared columns.
     */
    Collection<IndexColumn> columns();

    /**
     * Returns all index columns: user defined + implicitly attched.
     *
     * @return Indexed columns.
     */
    Collection<IndexColumn> indexedColumns();

    /**
     * Unique index flag.
     *
     * IMPORTANT: Index MUST have all affinity columns declared explicitely.
     *
     * @return Uniq flag.
     */
    default boolean unique() {
        return false;
    }
}
