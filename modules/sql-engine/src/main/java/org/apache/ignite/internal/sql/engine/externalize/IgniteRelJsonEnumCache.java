package org.apache.ignite.internal.sql.engine.externalize;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;

class IgniteRelJsonEnumCache {

    private final Map<String, Enum<?>> enums;

    private IgniteRelJsonEnumCache(Map<String, Enum<?>> enums) {
        this.enums = enums;
    }

    <E extends Enum<E>> E get(String name) {
        return (E) requireNonNull(enums.get(name));
    }

    <E extends Enum<E>> E resolveFrom(Object name) {
        assert name instanceof String && enums.containsKey(name);

        return get((String) name);
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private final Map<String, Enum<?>> enums = new HashMap<>();

        Builder register(Class<? extends Enum<?>> aclass) {
            String prefix = aclass.getSimpleName() + "#";
            for (Enum<?> enumConstant : aclass.getEnumConstants()) {
                enums.put(prefix + enumConstant.name(), enumConstant);
            }

            return this;
        }

        IgniteRelJsonEnumCache build() {
            return new IgniteRelJsonEnumCache(Map.copyOf(enums));
        }
    }
}
