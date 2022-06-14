package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.property.PropertiesHelper.createPropsByNameMap;

import java.util.Map;
import org.apache.ignite.internal.sql.engine.property.Property;
import org.jetbrains.annotations.Nullable;

/**
 * Enumerates the properties which might be used during query execution.
 */
public class QueryProperty {
    public static final Property<Long> QUERY_TIMEOUT = new Property<>("query_timeout", Long.class);
    public static final Property<String> DEFAULT_SCHEMA = new Property<>("default_schema", String.class);

    private static final Map<String, Property<?>> propsByName = createPropsByNameMap(QueryProperty.class);

    /** Returns a property for the given name or {@code null} if there is no property with such name. */
    public static @Nullable Property<?> byName(String name) {
        return propsByName.get(name);
    }
}
