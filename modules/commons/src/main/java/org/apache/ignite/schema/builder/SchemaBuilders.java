package org.apache.ignite.schema.builder;

import org.apache.ignite.internal.schema.builder.HashIndexBuilderImpl;
import org.apache.ignite.internal.schema.builder.PartialIndexBuilderImpl;
import org.apache.ignite.internal.schema.builder.SortedIndexBuilderImpl;

public class SchemaBuilders {
    public static SortedIndexBuilder sortedIndex(String name) {
        return new SortedIndexBuilderImpl().withName(name);
    }

    public static PartialIndexBuilder partialIndex(String name) {
        return new PartialIndexBuilderImpl().withName(name);
    }

    public static HashIndexBuilder hashIndex(String name) {
        return new HashIndexBuilderImpl().withName(name);
    }


    private SchemaBuilders() {
    }
}
