package org.apache.ignite.schema;

public interface TableIndex {
    /**
     * @return Index name.
     */
    String name();

    /**
     * Schema name + Index name
     *
     * @return Canonical index name.
     */
    String canonicalName();
}
