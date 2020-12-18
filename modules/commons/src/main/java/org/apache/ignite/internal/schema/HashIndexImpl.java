package org.apache.ignite.internal.schema;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.schema.HashIndex;

public class HashIndexImpl implements HashIndex {

    private final String name;

    public HashIndexImpl(String name) {
        this.name = name;
    }

    @Override public Collection<String> columns() {
        return Collections.emptyList();
    }

    @Override public String name() {
        return name;
    }
}
