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

package org.apache.ignite.internal.catalog.commands.altercolumn;

import static org.apache.ignite.lang.ErrorGroups.Sql.UNSUPPORTED_DDL_OPERATION_ERR;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Replaces {@code type} of the column descriptor according to the {@code ALTER COLUMN SET DATA TYPE} command.
 *
 * <p>The following changes are supported for non-PK columns:
 * <ul>
 *     <li>Precision increase for {@code DECIMAL}.</li>
 *     <li>Length increase for {@code VARCHAR} and {@code VARBINARY}.</li>
 *     <li>Type change: INT8 -> INT16 -> INT32 -> INT64</li>
 *     <li>Type change: FLOAT -> DOUBLE</li>
 * </ul>
 * All other modifications are rejected.
 */
public class AlterColumnType implements AlterColumnAction {
    private static final EnumSet<ColumnType> varLenTypes = EnumSet.of(ColumnType.STRING, ColumnType.BYTE_ARRAY);
    private static final Map<ColumnType, Set<ColumnType>> supportedTransitions = new EnumMap<>(ColumnType.class);

    static {
        supportedTransitions.put(ColumnType.INT8, EnumSet.of(ColumnType.INT16, ColumnType.INT32, ColumnType.INT64));
        supportedTransitions.put(ColumnType.INT16, EnumSet.of(ColumnType.INT32, ColumnType.INT64));
        supportedTransitions.put(ColumnType.INT32, EnumSet.of(ColumnType.INT64));
        supportedTransitions.put(ColumnType.FLOAT, EnumSet.of(ColumnType.DOUBLE));
    }

    private final ColumnType type;

    private final Integer precision;

    private final Integer scale;

    /** Default constructor. */
    public AlterColumnType(ColumnType type, Integer precision, Integer scale) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public @Nullable TableColumnDescriptor apply(TableColumnDescriptor origin, boolean isPkColumn) {
        boolean varLenType = varLenTypes.contains(type);

        if (origin.type() == type
                && (scale == null || origin.scale() == scale)
                && (precision == null
                        || (varLenType && origin.length() == precision)
                        || (type == ColumnType.DECIMAL && origin.precision() == precision)
                )
        ) {
            // No-op.
            return null;
        }

        if (isPkColumn) {
            throwException("Cannot change data type for primary key column '{}'.", origin.name(), origin.type(), type);
        }

        if (origin.type() != type) {
            Set<ColumnType> supportedTypes = supportedTransitions.get(origin.type());

            if (supportedTypes == null || !supportedTypes.contains(type)) {
                throwException("Cannot change data type for column '{}' [from={}, to={}].", origin.name(), origin.type(), type);
            }
        }

        if (precision != null) {
            if (varLenType) {
                if (precision < origin.length()) {
                    throwException("Cannot decrease length to {} for column '{}'.", precision, origin.name());
                }
            } else if (type == ColumnType.DECIMAL) {
                if (precision < origin.precision()) {
                    throwException("Cannot decrease precision to {} for column '{}'.", precision, origin.name());
                }
            } else {
                throwException("Cannot change precision to {} for column '{}'.", precision, origin.name());
            }
        }

        if (scale != null && origin.scale() != scale) {
            throwException("Cannot change scale to {} for column '{}'.", scale, origin.name());
        }

        return new TableColumnDescriptor(
                origin.name(),
                type,
                origin.nullable(),
                origin.defaultValue(),
                precision == null || varLenType
                        ? origin.precision()
                        : precision,
                origin.scale(),
                varLenType && precision != null
                        ? precision
                        : origin.length()
        );
    }

    private static void throwException(String msg, Object... params) {
        throw new SqlException(UNSUPPORTED_DDL_OPERATION_ERR, IgniteStringFormatter.format(msg, params));
    }
}
