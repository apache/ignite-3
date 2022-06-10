package org.apache.ignite.internal.sql.engine.property;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.Map;
import org.jetbrains.annotations.Nullable;

class PropertiesHolderImpl implements PropertiesHolder {
    private final Map<Property<?>, Object> props;

    PropertiesHolderImpl(Map<Property<?>, Object> props) {
        this.props = Map.copyOf(props);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public <T> @Nullable T get(Property<T> prop) {
        Object val = props.get(prop);

        if (val == null) {
            return null;
        }

        assert prop.cls.isAssignableFrom(val.getClass())
                : format("Unexpected property value [name={}, expCls={}, actCls={}, val={}]", prop.name, prop.cls, val.getClass(), val);

        return (T) prop.cls.cast(val);
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return props.size();
    }

    /** {@inheritDoc} */
    @Override
    public Map<Property<?>, Object> toMap() {
        return props;
    }
}
