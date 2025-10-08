package org.apache.ignite.internal.configuration.storage;

import java.io.Serializable;
import java.util.Map;

public interface WriteEntry {
    Map<String, ? extends Serializable> newValues();

    Map<String, ? extends Serializable> valuesWithFilteredDefaults();

    long version();
}
