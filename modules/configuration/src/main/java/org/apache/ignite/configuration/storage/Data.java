package org.apache.ignite.configuration.storage;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents data in configuration storage.
 */
public class Data {
    /** Values. */
    private final Map<String, Serializable> values;

    /** Configuration storage version. */
    private final int version;

    /**
     * Constructor.
     * @param values Values.
     * @param version Version.
     */
    public Data(Map<String, Serializable> values, int version) {
        this.values = values;
        this.version = version;
    }

    /**
     * Get values.
     * @return Values.
     */
    public Map<String, Serializable> values() {
        return values;
    }

    /**
     * Get version.
     * @return version.
     */
    public int version() {
        return version;
    }
}
