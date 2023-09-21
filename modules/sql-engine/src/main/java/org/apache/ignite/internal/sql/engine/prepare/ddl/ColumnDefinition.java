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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultLength;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.columnType;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** Defines a particular column within table. */
public class ColumnDefinition {
    private final String name;

    private final RelDataType type;

    private final DefaultValueDefinition defaultValueDefinition;

    private ColumnType colType;

    /**
     * Calcite definition {@link org.apache.calcite.sql.type.SqlTypeName} for precision and scale is
     * different from standard, this fixes such a case.
     **/
    private static final Set<SqlTypeName> PRECISION_ALLOWED = EnumSet.of(SqlTypeName.FLOAT, SqlTypeName.REAL);

    /** Creates a column definition. */
    public ColumnDefinition(String name, RelDataType type, DefaultValueDefinition defaultValueDefinition) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.defaultValueDefinition = Objects.requireNonNull(defaultValueDefinition, "defaultValueDefinition");
    }

    /**
     * Get column's name.
     */
    public String name() {
        return name;
    }

    /**
     * Get column's type.
     */
    public RelDataType type() {
        return type;
    }

    /**
     * Returns default value definition.
     *
     * @param <T> Desired subtype of the definition.
     * @return Default value definition.
     */
    @SuppressWarnings("unchecked")
    public <T extends DefaultValueDefinition> T defaultValueDefinition() {
        return (T) defaultValueDefinition;
    }

    /**
     * Get nullable flag: {@code true} if this column accepts nulls.
     */
    public boolean nullable() {
        return type.isNullable();
    }

    /**
     * Get column's precision.
     */
    public @Nullable Integer precision() {
        colType = Objects.requireNonNullElse(colType, Objects.requireNonNull(columnType(type()), "colType"));
        int prec = colType.specifiedLength() ? PRECISION_NOT_SPECIFIED : type.getPrecision();
        Integer ret = prec == PRECISION_NOT_SPECIFIED ? null : prec;

        return type.getSqlTypeName().allowsPrec() || PRECISION_ALLOWED.contains(type.getSqlTypeName()) ? ret : null;
    }

    /**
     * Get column's scale.
     */
    public @Nullable Integer scale() {
        colType = Objects.requireNonNullElse(colType, Objects.requireNonNull(columnType(type()), "colType"));
        int scale = colType.specifiedLength() ? SCALE_NOT_SPECIFIED : type.getScale();
        Integer ret = scale == SCALE_NOT_SPECIFIED ? null : scale;

        return type.getSqlTypeName().allowsScale() ? ret : null;
    }

    /**
     * Get column's length.
     */
    public @Nullable Integer length() {
        colType = Objects.requireNonNullElse(colType, Objects.requireNonNull(columnType(type()), "colType"));
        return colType.specifiedLength() ? defaultLength(colType, DEFAULT_LENGTH) : null;
    }
}
