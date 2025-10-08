package org.apache.ignite.internal.configuration.storage;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.util.Lazy;

public class WriteEntryImpl implements WriteEntry {
    private final Map<String, ? extends Serializable> defaults;

    private final Map<String, ? extends Serializable> allChanges;

    private final Lazy<Map<String, ? extends Serializable>> filtered = new Lazy<>(this::filteredDefaults);

    private final long version;

    public WriteEntryImpl(
            Map<String, ? extends Serializable> changes,
            long version
    ) {
        this(changes, Map.of(), version);
    }

    public WriteEntryImpl(
            Map<String, ? extends Serializable> allChanges,
            Map<String, ? extends Serializable> defaults,
            long version
    ) {
        this.defaults = defaults;
        this.allChanges = allChanges;
        this.version = version;
    }

    @Override
    public Map<String, ? extends Serializable> newValues() {
        return Collections.unmodifiableMap(allChanges);
    }

    @Override
    public Map<String, ? extends Serializable> valuesWithFilteredDefaults() {
        return filtered.get();
    }

    @Override
    public long version() {
        return version;
    }

    private Map<String, ? extends Serializable> filteredDefaults() {
        Map<String, ? extends Serializable> result = new HashMap<>(allChanges);
        defaults.forEach((key, defaultValue) -> {
            Serializable change = result.get(key);
            if (Objects.deepEquals(change, defaultValue)) {
                result.put(key, null);
            }
        });
        return result;
    }
}
