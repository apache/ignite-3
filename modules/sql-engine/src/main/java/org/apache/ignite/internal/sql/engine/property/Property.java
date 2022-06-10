package org.apache.ignite.internal.sql.engine.property;

import java.util.Objects;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

public class Property<T> {
    @IgniteToStringInclude
    public final String name;
    @IgniteToStringInclude
    public final Class<?> cls;

    public Property(String name, Class<T> cls) {
        this.name = Objects.requireNonNull(name, "name");
        this.cls = Objects.requireNonNull(cls, "cls");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Property<?> property = (Property<?>) o;

        return name.equals(property.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return S.toString(Property.class, this);
    }
}
