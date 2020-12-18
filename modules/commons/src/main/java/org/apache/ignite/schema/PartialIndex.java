package org.apache.ignite.schema;

/**
 * Partial index descriptor.
 */
public interface PartialIndex extends SortedIndex {
    /**
     * Partial index expression.
     *
     * @return Expresssion.
     */
    String expr();
}
