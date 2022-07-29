package org.apache.ignite.internal.sql.engine.sql;

/**
 * Enumeration of supported index types.
 */
public enum IgniteSqlIndexType {
    /** Sorted index. */
    TREE,

    /** Hash index. */
    HASH,

    /** The user have omitted USING clause, hence the type is set to {@link #TREE} implicitly. */
    IMPLICIT_TREE
}
