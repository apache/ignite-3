package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.TableIndex;

public interface IndexBuilder {
    /**
     * @return Table index.
     */
    TableIndex build();
}
