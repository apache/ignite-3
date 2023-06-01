package org.apache.ignite.internal.catalog.commands.altercolumn;

import org.apache.ignite.sql.ColumnType;

public class AlterColumnTypeParams {
    private ColumnType type;

    private Integer precision;

    private Integer scale;

    /**
     * Constructor.
     *
     * @param type Column type.
     * @param precision Column precision.
     * @param scale Column scale.
     */
    public AlterColumnTypeParams(ColumnType type, Integer precision, Integer scale) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
    }

    private AlterColumnTypeParams() {

    }

    public ColumnType type() {
        return type;
    }

    public Integer precision() {
        return precision;
    }

    public Integer scale() {
        return scale;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private AlterColumnTypeParams params = new AlterColumnTypeParams();

        public Builder type(ColumnType type) {
            params.type = type;

            return this;
        }

        public Builder precision(int precision) {
            params.precision = precision;

            return this;
        }

        public Builder scale(int scale) {
            params.scale = scale;

            return this;
        }

        public AlterColumnTypeParams build() {
            AlterColumnTypeParams params0 = params;

            params = null;

            return params0;
        }
    }
}
