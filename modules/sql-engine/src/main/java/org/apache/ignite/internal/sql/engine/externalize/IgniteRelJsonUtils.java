package org.apache.ignite.internal.sql.engine.externalize;

import static org.apache.ignite.internal.util.IgniteUtils.igniteClassLoader;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

final class IgniteRelJsonUtils {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    @Nullable
    static Class<?> classForNameOrNull(String typeName) {
        try {
            return IgniteUtils.forName(typeName, igniteClassLoader());
        } catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    static Class<?> classForName(String typeName) {
        try {
            return IgniteUtils.forName(typeName, igniteClassLoader());
        } catch (ClassNotFoundException ignored) {
            throw new IgniteInternalException(INTERNAL_ERR, "RelJson unable to load type: " + typeName);
        }
    }

}
