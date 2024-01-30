/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.prepare;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;

/**
 * Dynamic parameter metadata.
 */
public final class ParameterType {

    private final ColumnType columnType;

    private final int precision;

    private final int scale;

    private final boolean nullable;

    /** Constructor. */
    public ParameterType(ColumnType columnType, int precision, int scale, boolean nullable) {
        this.columnType = columnType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    /**
     * {@link ColumnType} of this parameter.
     */
    public ColumnType columnType() {
        return columnType;
    }

    /**
     * Returns precision of parameter type, if applicable.
     *
     * @see ColumnMetadata#precision()
     */
    public int precision() {
        return precision;
    }

    /**
     * Returns scale of this parameter type, if applicable.
     *
     * @see ColumnMetadata#scale()
     */
    public int scale() {
        return scale;
    }

    /** Returns {@code true} if parameter is nullable and {@code false} otherwise. */
    public boolean nullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }

    /** Creates parameter metadata from the given logical type. */
    static ParameterType fromRelDataType(RelDataType type) {
        ColumnType columnType = TypeUtils.columnType(type);
        assert columnType != null : "No column type for " + type;

        int precision = columnType.lengthAllowed() || columnType.precisionAllowed()
                ? type.getPrecision()
                : ColumnMetadata.UNDEFINED_PRECISION;

        int scale = columnType.scaleAllowed() ? type.getScale() : ColumnMetadata.UNDEFINED_SCALE;

        return new ParameterType(columnType, precision, scale, type.isNullable());
    }
}
