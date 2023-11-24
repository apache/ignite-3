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

package org.apache.ignite.internal.table.distributed.replicator;

import static org.apache.ignite.sql.ColumnType.DATE;
import static org.apache.ignite.sql.ColumnType.DATETIME;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.DOUBLE;
import static org.apache.ignite.sql.ColumnType.FLOAT;
import static org.apache.ignite.sql.ColumnType.INT16;
import static org.apache.ignite.sql.ColumnType.INT8;
import static org.apache.ignite.sql.ColumnType.NUMBER;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.apache.ignite.sql.ColumnType.TIME;

import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.sql.ColumnType;

/**
 * Utility to determine type convertibility.
 */
public class TypeConvertibility {
    /**
     * Returns whether type change from old to new column version is lossless (that is, no information
     * is lost during conversion).
     *
     * @param oldColumn Old column descriptor.
     * @param newColumn New column descriptor.
     */
    // TODO: IGNITE-20956 - Unify this with Catalog rules.
    public static boolean typeChangeIsLossless(CatalogTableColumnDescriptor oldColumn, CatalogTableColumnDescriptor newColumn) {
        ColumnType oldType = oldColumn.type();
        ColumnType newType = newColumn.type();

        // Int to Int.
        if (oldType.integral() && newType.integral()) {
            return oldType.ordinal() <= newType.ordinal();
        }

        // Int to floating point.
        if ((oldType == INT8 || oldType == INT16) && newType.floatingPoint()) {
            return true;
        }

        if (oldType == FLOAT && newType == DOUBLE) {
            return true;
        }

        // Time/date to Datetime.
        if ((oldType == TIME || oldType == DATE) && newType == DATETIME) {
            return true;
        }

        // Int <-> NUMBER/DECIMAL.
        if (oldType.integral() && (newType == NUMBER || newType == DECIMAL) && integralMaxDigits(oldType) <= maxExactIntPlaces(newColumn)) {
            return true;
        }
        if ((oldType == NUMBER || oldType == DECIMAL) && newType.integral() && oldColumn.scale() <= 0
                && oldColumn.precision() - oldColumn.scale() < integralMaxDigits(newType)) {
            return true;
        }

        // NUMBER <-> DECIMAL.
        if ((oldType == NUMBER && newType == DECIMAL || oldType == DECIMAL && newType == NUMBER)
                && oldColumn.scale() <= newColumn.scale()
                && oldColumn.precision() - oldColumn.scale() <= newColumn.precision() - newColumn.scale()) {
            return true;
        }

        // Increasing length.
        if (oldType == newType && oldType.lengthAllowed()
                && oldColumn.length() <= newColumn.length()
                && oldColumn.scale() == newColumn.scale()
                && oldColumn.precision() == newColumn.precision()) {
            return true;
        }

        // Increasing precision.
        if (oldType == newType && oldType.precisionAllowed()
                && oldColumn.length() == newColumn.length()
                && oldColumn.scale() == newColumn.scale()
                && oldColumn.precision() <= newColumn.precision()) {
            return true;
        }

        // To STRING.
        if (oldType.convertsToStringLosslessly() && newType == STRING && maxCharacters(oldColumn) <= newColumn.length()) {
            return true;
        }

        return false;
    }

    private static int integralMaxDigits(ColumnType integralType) {
        switch (integralType) {
            case INT8:
                return 3;
            case INT16:
                return 5;
            case INT32:
                return 9;
            case INT64:
                return 17;
            default:
                throw new IllegalArgumentException("Unsupported type " + integralType);
        }
    }

    private static int maxExactIntPlaces(CatalogTableColumnDescriptor column) {
        switch (column.type()) {
            case DECIMAL:
                if (column.scale() >= 0) {
                    return column.precision() - column.scale();
                } else {
                    // This emplies a lossy rounding, so no exact int places at all.
                    return 0;
                }
            case NUMBER:
                if (column.scale() >= 0) {
                    return column.precision();
                } else {
                    // This emplies a lossy rounding, so no exact int places at all.
                    return 0;
                }
            default:
                throw new IllegalArgumentException("Unsupported type " + column.type());
        }
    }

    private static int maxCharacters(CatalogTableColumnDescriptor column) {
        if (column.type().integral()) {
            // 1 is for sign.
            return integralMaxDigits(column.type()) + 1;
        }

        switch (column.type()) {
            case DECIMAL:
            case NUMBER:
                if (column.scale() > 0) {
                    // 1 is for period, and 1 for sign.
                    return column.precision() + 2;
                } else {
                    // 1 is for sign.
                    return column.precision() - column.scale() + 1;
                }
            case UUID:
                return 36;
            case STRING:
                return column.length();
            default:
                throw new IllegalArgumentException("Unsupported type " + column.type());
        }
    }
}
