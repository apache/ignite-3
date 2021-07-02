package org.apache.ignite.internal.schema.mapping;

import java.io.Serializable;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * Column mapping helper.
 */
public class ColumnMapping {
    /** Identity mapper. */
    private static final IdentityMapper IDENTITY_MAPPER = new IdentityMapper();

    /**
     * @return Identity mapper instance.
     */
    public static ColumnMapper identityMapping() {
        return IDENTITY_MAPPER;
    }

    /**
     * @param cols Number of columns.
     */
    public static ColumnaMapperBuilder mapperBuilder(int cols) {
        return new ColumnMapperImpl(cols);
    }

    /**
     * Builds mapper for given schema via merging schema mapper with the provided one.
     * Used for builing columns mapper between arbitraty schema versions with bottom-&gt;top approach.
     *
     * @param mapping Column mapper.
     * @param schema Target schema.
     * @return Merged column mapper.
     */
    public static ColumnMapper mergeMapping(ColumnMapper mapping, SchemaDescriptor schema) {
        if (mapping == identityMapping())
            return schema.columnMapping();
        else if (schema.columnMapping() == identityMapping())
            return mapping;

        ColumnaMapperBuilder builder = mapperBuilder(schema.length());

        ColumnMapper schemaMapper = schema.columnMapping();

        for (int i = 0; i < schema.length(); i++) {
            int idx = schemaMapper.map(i);

            if (idx < 0)
                builder.add(i, -1);
            else
                builder.add(i, mapping.map(idx));
        }

        return builder.build();
    }

    /**
     * Stub.
     */
    private ColumnMapping() {
    }

    /**
     * Identity column mapper.
     */
    private static class IdentityMapper implements ColumnMapper, Serializable {
        /** {@inheritDoc} */
        @Override public int map(int idx) {
            return idx;
        }
    }
}
