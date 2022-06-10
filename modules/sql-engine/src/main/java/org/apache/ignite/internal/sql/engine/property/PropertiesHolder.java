package org.apache.ignite.internal.sql.engine.property;

import java.util.Map;
import org.jetbrains.annotations.Nullable;

public interface PropertiesHolder {
    <T> @Nullable T get(Property<T> prop);

    int size();

    Map<Property<?>, Object> toMap();

    static PropertiesHolder holderFor(Map<Property<?>, Object> values) {
        for (Map.Entry<Property<?>, Object> e : values.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }

            if (!e.getKey().cls.isAssignableFrom(e.getValue().getClass())) {
                throw new IllegalArgumentException();
            }
        }

        return new PropertiesHolderImpl(Map.copyOf(values));
    }
}
