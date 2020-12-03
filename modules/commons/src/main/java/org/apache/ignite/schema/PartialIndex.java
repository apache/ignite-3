package org.apache.ignite.schema;

public interface PartialIndex extends SortedIndex {
    /**
     * Partial index expression.
     *
     * @return Expresssion.
     */
    String expr();
}
