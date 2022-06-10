package org.apache.ignite.internal.sql.engine.property;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.sql.engine.QueryProperty;

final public class PropertiesHelper {
    private PropertiesHelper() {}

    public static Map<String, Property<?>> createPropsByNameMap(Class<?> cls) {
        Map<String, Property<?>> tmp = new HashMap<>();

        for (Field f : cls.getDeclaredFields()) {
            if (!Property.class.equals(f.getType())
                    || !Modifier.isPublic(f.getModifiers())) {
                continue;
            }

            try {
                Property<?> prop = (Property<?>) f.get(QueryProperty.class);

                tmp.put(prop.name, prop);
            } catch (IllegalAccessException e) {
                // should not happen
                throw new AssertionError(e);
            }
        }

        return Map.copyOf(tmp);
    }
}
